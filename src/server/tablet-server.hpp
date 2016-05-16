#ifndef __tablet_server_hpp__
#define __tablet_server_hpp__

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

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <set>
#include <map>

#include "geeps-user-defined-types.hpp"
#include "common/common-util.hpp"
#include "common/row-op-util.hpp"
#include "server-encoder-decoder.hpp"
#include "metadata-server.hpp"

using std::string;
using std::vector;
using boost::shared_ptr;

class ServerClientEncode;
class MetadataServer;   /* Used in get_stats() */

class TabletStorage {
 public:
  struct Stats {
    int64_t nr_request;
    int64_t nr_request_prior;
    int64_t nr_local_request;
    int64_t nr_send;
    int64_t nr_update;
    int64_t nr_local_update;
    int64_t nr_rows;
    double send_data_time;
    double clock_ad_apply_op_time;
    double clock_ad_send_pending_time;
    double clock_ad_time_tot;
    double iter_var_time;
    double inc_time;

    void reset() {
      nr_request = 0;
      nr_request_prior = 0;
      nr_local_request = 0;
      nr_send = 0;
      nr_update = 0;
      nr_local_update = 0;
      send_data_time = 0.0;
      clock_ad_time_tot = 0.0;
      clock_ad_apply_op_time = 0.0;
      clock_ad_send_pending_time = 0.0;
      inc_time = 0.0;
      iter_var_time = 0.0;
    }

    Stats() {
      reset();
    }

    Stats& operator += (const Stats& rhs) {
      return *this;
    }
    std::string to_json() {
      std::stringstream ss;
      ss << "{"
         << "\"nr_rows\": " << nr_rows << ", "
         << "\"nr_request\": " << nr_request << ", "
         << "\"nr_request_prior\": " << nr_request_prior << ", "
         << "\"nr_local_request\": " << nr_local_request << ", "
         << "\"nr_send\": " << nr_send << ", "
         << "\"nr_update\": " << nr_update << ", "
         << "\"nr_local_update\": " << nr_local_update << ", "
         << "\"send_data_time\": " << send_data_time << ", "
         << "\"clock_ad_apply_op_time\": " << clock_ad_apply_op_time << ", "
         << "\"clock_ad_send_pending_time\": "
         << clock_ad_send_pending_time << ", "
         << "\"clock_ad_time_tot\": " << clock_ad_time_tot << ", "
         << "\"iter_var_time\": " << iter_var_time << ", "
         << "\"inc_time\": " << inc_time
         << " } ";
      return ss.str();
    }
  };
  Stats server_stats;

  typedef boost::unordered_map<TableRow, uint> Row2Index;

  typedef boost::unordered_map<iter_t, RowKeys> PendingReadsLog;
  typedef std::vector<PendingReadsLog> MulticlientPendingReadsLog;
  typedef std::vector<iter_t> VectorClock;

  struct DataTable {
    VectorClock vec_clock;
    iter_t global_clock;
    Row2Index row2idx;
    size_t row_count;
    DataStorage store;
  };
  typedef std::vector<DataTable> DataTables;

 private:
  uint channel_id;
  uint num_channels;
  uint process_id;
  uint num_processes;
  uint num_clients;

  boost::unordered_map<std::string, table_id_t> table_directory;

  shared_ptr<ServerClientEncode> communicator;

  cudaStream_t cuda_stream;
  cublasHandle_t cublas_handle;

  DataTables data_tables;

  GeePsConfig config;

  tbb::tick_count start_time;
  tbb::tick_count first_come_time;

 private:
  template<class T>
  void resize_storage(vector<T>& storage, uint size) {
    if (storage.capacity() <= size) {
      uint capacity = get_nearest_power2(size);
      storage.reserve(capacity);
      // cerr << "capacity is " << capacity << endl;
    }
    if (storage.size() <= size) {
      storage.resize(size);
      // cerr << "size is " << size << endl;
    }
  }
  void process_multiclient_pending_reads(
      iter_t clock, uint table_id);
  void process_pending_reads(
      uint client_id, iter_t clock, uint table_id);
  void apply_updates(
      uint table_id, RowOpVal *update_rows, size_t batch_size);

 public:
  TabletStorage(
      uint channel_id, uint num_channels, uint process_id, uint num_processes,
      shared_ptr<ServerClientEncode> communicator,
      cudaStream_t cuda_stream, cublasHandle_t cublas_handle,
      const GeePsConfig& config);
  void update_row_batch(
      uint client_id, iter_t clock, uint table_id,
      RowKey *row_keys, RowOpVal *updates, uint batch_size);
  void clock(uint client_id, iter_t clock, uint table_id);
  void get_stats(
      uint client_id, shared_ptr<MetadataServer> metadata_server);
    /* Now it also needs the stats from the MetadataServer.
     * This is just a work-around, and we need to fix it in the future.
     */
  void reset_perf_counters();
};


void server(uint channel_id, uint nr_channels, uint server_id, uint nr_servers,
            boost::shared_ptr<zmq::context_t> zmq_ctx,
            const GeePsConfig& config);

#endif  // defined __tablet_server_hpp__
