#ifndef __clientlib_hpp__
#define __clientlib_hpp__

/*
 * Copyright (c) 2016, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <tbb/tick_count.h>
#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <set>
#include <string>
#include <vector>
#include <map>
#include <utility>

#include "geeps.hpp"
#include "common/common-util.hpp"
#include "common/row-op-util.hpp"
#include "common/router-handler.hpp"
#include "common/work-pusher.hpp"
#include "server/server-entry.hpp"
#include "stats-tracker.hpp"

using std::string;
using std::vector;
using std::pair;
using std::cerr;
using std::cout;
using std::endl;
using std::make_pair;
using boost::ref;
using boost::format;
using boost::bind;
using boost::make_shared;
using boost::shared_ptr;
using boost::unique_lock;
using boost::mutex;
using boost::condition_variable;


class ServerClientDecode;
class ClientServerEncode;

struct FlatOps : DataStorage {
  enum {
    NONE,
    INC,
  } flag;
  void reset() {
    flag = NONE;
  }
  FlatOps() : DataStorage() {
    reset();
  }
};

struct OpMemBufferPool {
  bool gpu_;
  size_t size_;
  size_t free_start_;
  size_t free_end_;
  size_t num_free_;
  std::vector<FlatOps *> op_mems;
  boost::mutex mutex_;
  boost::condition_variable cvar_;
  OpMemBufferPool() :
      gpu_(false), size_(0), free_start_(0), free_end_(0), num_free_(0) {}
  void init(bool gpu, size_t size, size_t flatops_size) {
    boost::unique_lock<boost::mutex> lock(mutex_);
    gpu_ = gpu;
    size_ = size;
    free_start_ = 0;
    free_end_ = size - 1;
    num_free_ = size;
    if (op_mems.size()) {
      for (size_t i = 0; i < op_mems.size(); i++) {
        delete op_mems[i];
      }
    }
    op_mems.resize(size);
    for (size_t i = 0; i < size; i++) {
      op_mems[i] = new FlatOps();
      if (gpu_) {
        op_mems[i]->init(flatops_size, DataStorage::GPU);
        /* We didn't zerofy the data here,
         * because we assume the flat_op data will be zerofied when used */
      } else {
        op_mems[i]->init(flatops_size, DataStorage::CPU);
      }
    }
  }
  FlatOps *get() {
    boost::unique_lock<boost::mutex> lock(mutex_);
    // CHECK_GT(num_free_, 0);
    while (num_free_ == 0) {
      // cvar_.wait(lock);
      if (!cvar_.timed_wait(lock,
          boost::posix_time::milliseconds(12000))) {
        cerr << "OpMemBufferPool waits for more space timed out\n";
      }
    }
    CHECK_LT(free_start_, size_);
    FlatOps *free_entry = op_mems[free_start_];
    free_start_ = (free_start_ + 1) % size_;
    num_free_--;
    return free_entry;
  }
  void put(FlatOps *flat_ops) {
    boost::unique_lock<boost::mutex> lock(mutex_);
    CHECK(size_);
    free_end_ = (free_end_ + 1) % size_;
    num_free_++;
    CHECK_EQ(op_mems[free_end_], flat_ops);
    cvar_.notify_all();
  }
  ~OpMemBufferPool() {
    if (op_mems.size()) {
      for (size_t i = 0; i < op_mems.size(); i++) {
        delete op_mems[i];
      }
    }
  }
};

typedef std::vector<FlatOps *> Oplog; /* Indexed by clock */

struct ParamCacheMetadata {
  size_t cache_idx;
  int tablet_server_id;
  size_t row_idx;   /* oplog index */
};
typedef boost::unordered_map<TableRow, ParamCacheMetadata> ParamCacheIndex;

struct PerChannelIndexInfo {
  size_t index_start;
  size_t index_size;
};

struct SharedBuffer {
  RowData *buffer;
  size_t num_rows;
  int ref_count;
  bool need_keep;
  boost::mutex mutex;
  boost::condition_variable cvar;
  SharedBuffer() :
      buffer(NULL), num_rows(0), ref_count(0), need_keep(false) {}
};

struct OpDataBuffer {
  RowData *buffer;
  bool buffer_being_used;
  bool updates_ready;
  boost::mutex mutex;
  boost::condition_variable cvar;
  OpDataBuffer() {
    init();
  }
  OpDataBuffer& operator=(const OpDataBuffer& cc) {
    /* Uncopiable */
    return *this;
  }
  OpDataBuffer(const OpDataBuffer& copy) {
    /* Uncopiable */
    init();
  }
  void init() {
    buffer = NULL;
    buffer_being_used = false;
    updates_ready = false;
  }
};

typedef GpuCache<MultithreadedCacheHelper> ThreadCache;

typedef DataStorage LocalStorage;

struct OpInfo {
  enum OpType {
    READ,
    POST_READ,
    PRE_WRITE,
    WRITE,
    CLOCK,
  } type;
  table_id_t table;
  std::vector<row_idx_t> rows;
  uint table_id;
  iter_t slack;
  size_t num_vals_limit;
  int prestep_handle;
  bool local;
  bool fetch_local;   /* For local access only */
  bool keep_local;    /* For local access only */
  enum IndexType {
    SINGLE_INDEX,
    DOUBLE_INDEX,
  } index_type;
  size_t row_index_size;
  DoubleIndex *row_index_gpu;
  DoubleIndex *row_index_cpu;
  std::vector<PerChannelIndexInfo> index_infos;
  DataStorage::MemoryType data_location;
  bool table_last_write;
  std::vector<OpDataBuffer> op_data_buffers;   /* One buffer for every clock */
  SharedBuffer *shared_buffer;  /* Shared buffer is only used by local ops */
  iter_t last_finished_clock;
      /* No lock protecting this, because it's only used
       * by the background worker threads */
  RowData *local_storage_ptr;
  double read_time;
  double fetch_time;
  double alloc_wait_time;
  double write_time;
  double keep_time;
  double reclaim_wait_time;
  size_t rows_fetched;
  size_t rows_kept;
  OpInfo(OpType optype,
      int prestep_handle, bool local = false, bool keep_local = false)
      : type(optype), prestep_handle(prestep_handle),
        local(local), keep_local(keep_local) {
    /* Only for POST_READ and WRITE */
    CHECK(type == POST_READ || type == WRITE);
    init();
  }
  OpInfo(OpType optype)
    : type(optype) {
    /* Only for CLOCK */
    CHECK_EQ(type, CLOCK);
    init();
  }
  OpInfo(OpType optype,
      table_id_t table, const std::vector<row_idx_t> rows,
      iter_t slack = 0,
      size_t num_vals_limit = std::numeric_limits<size_t>::max(),
      bool local = false,
      bool fetch_local = false)
      : type(optype), table(table), rows(rows), slack(slack),
        num_vals_limit(num_vals_limit), local(local),
        fetch_local(fetch_local) {
    init();
  }
  void init() {
    table_id = table;
        /* Assuming table keys are consecutive numbers from zero */
    row_index_size = 0;
    row_index_gpu = NULL;
    row_index_cpu = NULL;
    last_finished_clock = INITIAL_DATA_AGE;
    read_time = 0;
    fetch_time = 0;
    alloc_wait_time = 0;
    write_time = 0;
    keep_time = 0;
    reclaim_wait_time = 0;
    rows_fetched = 0;
    rows_kept = 0;
  }
};

typedef std::vector<OpInfo> OpSeq;

struct OpSeqWorkerInfo {
  OpSeq *opseq_ptr;
  ThreadCache *thread_cache_ptr;
  size_t cpu_buffer_size;
};

struct ThreadData {
  uint thread_id;
  iter_t iteration;
      /* TODO: this should be a vector of clocks (one for each table) */
  Stats thread_stats;

  /* For background opseq worker */
  bool bg_worker_started;
  OpSeqWorkerInfo alloc_worker_info;
  boost::shared_ptr<boost::thread> alloc_worker_thread;
  OpSeqWorkerInfo reclaim_worker_info;
  boost::shared_ptr<boost::thread> reclaim_worker_thread;

  /* Local storage */
  LocalStorage local_storage_cpu;
  LocalStorage local_storage_gpu;

  size_t ngr_used;
  size_t thread_cache_size;

  /* Operation sequence, collected by the virtual iteration */
  OpSeq opseq;
  RowKeys row_keys_cpu;
  RowKeys row_keys_gpu;
  RowKeys local_row_keys_cpu;
  RowKeys local_row_keys_gpu;
  ThreadCache thread_cache;
  int last_handle;

  cudaStream_t cuda_stream;
  cublasHandle_t cublas_handle;
  RowData *cpu_buffer;
  size_t cpu_buffer_size;
};

typedef DataStorage DataCache;

struct ParamCache {
  size_t num_rows;
  ParamCacheIndex param_cache_index;
  DataCache data_cache;
  Oplog oplog;
  OpMemBufferPool opmem_buffer_pool;
  std::vector<size_t> per_server_row_start;
  std::vector<size_t> per_server_num_rows;
  // FlatOps sample_oplog;
  ParamCache() {}
  ParamCache(const ParamCache& other) {}
  ParamCache& operator=(const ParamCache& other) {
    /* Just to make std::vector happy */
    return *this;
  }
};

struct CachedTable {
  ParamCache param_cache_gpu;
  ParamCache param_cache_cpu;
  iter_t data_age;
  std::vector<iter_t> per_server_data_age;
  std::vector<iter_t> server_clock;
  iter_t server_clock_min;
  CachedTable() {}
  CachedTable(const CachedTable& other) {}
  CachedTable& operator=(const CachedTable& other) {
    /* Just to make std::vector happy */
    return *this;
  }
};

typedef std::vector<CachedTable> CachedTables;

struct CommunicationChannel {
  /* Communication stack */
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  boost::shared_ptr<RouterHandler> router;
  boost::shared_ptr<ClientServerEncode> encoder;
  boost::shared_ptr<ServerClientDecode> decoder;
  boost::shared_ptr<WorkPusher> work_pusher;
  boost::shared_ptr<boost::thread> bg_worker_thread;
  boost::shared_ptr<boost::thread> server_thread;
  /* Shared Cache */
  CachedTables cached_tables;
  BgthreadStats bgthread_stats;
  /* For the virtual iteration */
  iter_t pf_slack;
  size_t num_oplog_entries;
  size_t comm_buffer_size;
  RowData *send_buffer;
  RowData *recv_buffer;
  cudaStream_t cuda_stream_recv;
  cublasHandle_t cublas_handle_recv;
  cudaStream_t cuda_stream_send;
  cublasHandle_t cublas_handle_send;
  boost::shared_ptr<boost::mutex> mutex;
  boost::shared_ptr<boost::condition_variable> cvar;
  CommunicationChannel& operator=(const CommunicationChannel& cc) {
    /* STL vector needs that to be defined, but it shouldn't
     * actually be called.
     */
    assert(0);
    return *this;
  }
  CommunicationChannel(const CommunicationChannel& copy) {
    /* Uncopiable */
    /* STL vector needs that, and will call this function even though
     * we are not copying anything. So we just leave it blank.
     */
    // assert(0);
  }
  CommunicationChannel() {
    /* Should be constructed mannually */
  }
};


class ClientLib;
extern ClientLib *client_lib;  // singleton

class ClientLib {
  struct bgcomm_clock_msg_t {
    iter_t clock;
    uint table_id;
  };

  Stats proc_stats;

  std::vector<size_t> rows_per_channel;   /* Indexed by table_id */
  std::vector<CommunicationChannel> comm_channels;
  uint num_channels;

  uint process_id;

  /* Thread_management */
  boost::mutex global_mutex;
  boost::condition_variable global_cvar;

  boost::thread_specific_ptr<ThreadData> thread_data;
  uint nr_threads;

  /* Clock for this client process */
  iter_t current_iteration;
  iter_t fast_clock;

  /* Server states */
  vector<string> host_list;
  vector<uint> port_list;
  uint tcp_base_port;
  uint num_servers;

  /* Memory usage */
  size_t ngr_capacity;

  /* Config */
  const GeePsConfig config;

 private:
  /* This class allows a singleton object. Hence the private constructor. */
  ClientLib(uint process_id, const GeePsConfig& config);

  void init_comm_channel(
        uint channel_id, const GeePsConfig& config);
  uint get_channel_id(table_id_t table, row_idx_t row_id);
  uint get_channel_id(const TableRow& table_row);
  void create_oplog_entry(
      uint channel_id, iter_t clock, uint table_id, bool gpu);
  void reclaim_oplog(
      uint channel_id, iter_t start_clock, iter_t end_clock, uint table_id,
      bool gpu);
  int read_row_batch_param_cache(
      RowData *buffer, OpInfo& op_info, iter_t clock,
      cudaStream_t& cuda_stream, RowData *cpu_buffer,
      bool stat, bool timing);
  void read_batch_gpu(
      RowData *buffer, OpInfo& op_info, uint channel_id,
      cudaStream_t& cuda_stream);
  void read_batch_cpu(
      RowData *buffer, OpInfo& op_info, uint channel_id);
  bool update_batch_param_cache(
      OpInfo& op_info, const RowOpVal *updates, iter_t clock,
      cudaStream_t& cuda_stream, RowData *cpu_buffer, bool stat, bool timing);
  void update_batch_gpu(
      OpInfo& op_info, const RowOpVal *updates, uint channel_id, iter_t clock,
      cudaStream_t& cuda_stream);
  void update_batch_cpu(
      OpInfo& op_info, const RowOpVal *updates, uint channel_id, iter_t clock);
  void clock_all(iter_t clock);
  void clock_table(iter_t clock, uint table_id);
  void push_clock_work(
      uint channel_id, iter_t clock, uint table_id);
  bool recv_row_batch(
      uint channel_id, uint server_id, uint table_id,
      RowKey *row_keys, RowData *row_data,
      uint batch_size, iter_t data_age, iter_t self_clock, bool timing);
  void recv_row_batch_gpu(
      uint channel_id, uint server_id, RowKey *row_keys, RowData *row_data,
      uint batch_size, iter_t data_age, iter_t self_clock, uint table_id);
  void recv_row_batch_cpu(
      uint channel_id, uint server_id, RowKey *row_keys, RowData *row_data,
      uint batch_size, iter_t data_age, iter_t self_clock, uint table_id);
  void push_updates_param_cache(
      uint channel_id, iter_t clock, uint table_id);
  void read_batch_local(RowData *buffer, OpInfo& op_info,
      cudaStream_t& cuda_stream, RowData *cpu_buffer);
  void update_batch_local(OpInfo& op_info, const RowOpVal *updates,
      cudaStream_t& cuda_stream, RowData *cpu_buffer);

  void vi_thread_summarize();
  void vi_create_local_storage();
  void vi_decide_param_cache();
  void vi_create_thread_cache();
  void vi_process_channel_finalize(
      ThreadData& thread_data_ref, uint channel_id, bool gpu);
  void vi_process_channel_table_finalize(
      ThreadData& thread_data_ref, uint channel_id, uint table_id, bool gpu);
  void vi_process_finalize();
  void vi_thread_finalize();
  void vi_create_double_index(OpInfo& opinfo);

 public:
  static void CreateInstance(uint process_id, const GeePsConfig& config) {
    if (client_lib == NULL) {
      client_lib = new ClientLib(process_id, config);
    }
  }

  /* Routines called by application code */
  std::string json_stats();
  void shutdown();
  void thread_start();
  void thread_stop();
  iter_t get_clock();

  /* Callback functions */
  void find_row_cbk(
      table_id_t table, row_idx_t row_id, uint32_t tablet_server_id);
  void get_stats_cbk(const string& server_stats);
  void recv_row_batch_cbk(
      uint channel_id, iter_t data_age, iter_t self_clock, uint server_id,
      uint table_id, RowKey *row_keys, RowData *row_data, uint batch_size);
  void server_clock_cbk(
      uint channel_id, uint server_id, iter_t iter, uint table_id);
  void cbk_iterate(uint channel_id, vector<ZmqPortableBytes>& msgs);
  void push_updates(
      uint channel_id, iter_t clock, uint table_id);
  void alloc_worker_entry(OpSeqWorkerInfo& worker_info);
  void reclaim_worker_entry(OpSeqWorkerInfo& worker_info);
  void alloc_worker_localaccess(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void alloc_worker_read(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void alloc_worker_preupdate(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void reclaim_worker_postlocalaccess(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void reclaim_worker_postread(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void reclaim_worker_update(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);
  void reclaim_worker_clock(
      OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle);

  /* Helper functions */
  void iterate();
  iter_t get_iteration();

  /* Interfaces for virtual iteration */
  int virtual_read_batch(
      table_id_t table, const vector<row_idx_t>& row_ids, iter_t staleness,
      size_t num_vals_limit);
  int virtual_postread_batch(int prestep_handle);
  int virtual_prewrite_batch(
      table_id_t table, const vector<row_idx_t>& row_ids,
      size_t num_vals_limit);
  int virtual_write_batch(int prestep_handle);
  int virtual_localaccess_batch(
      table_id_t table, const vector<row_idx_t>& row_ids,
      size_t num_vals_limit, bool fetch);
  int virtual_postlocalaccess_batch(int prestep_handle, bool keep);
  int virtual_clock();
  int virtual_op(const OpInfo& opinfo);
  void finish_virtual_iteration();
  void start_gather_info();
  void finish_gather_info();

  /* Interfaces for GeePS */
  void start_opseq();
  bool read_batch(
      RowData **buffer_mem_ret, int handle, bool timing = true);
  void postread_batch(int handle);
  void preupdate_batch(RowOpVal **updates_mem_ret, int handle, bool stat = true);
  void update_batch(int handle);
  bool localaccess_batch(
      RowData **buffer_mem_ret, int handle);
  void postlocalaccess_batch(int handle);
};

#endif  // defined __clientlib_hpp__
