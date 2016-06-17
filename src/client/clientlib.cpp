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

#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>

#include <tbb/tick_count.h>

#include <glog/logging.h>

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "common/work-pusher.hpp"
#include "common/background-worker.hpp"
#include "common/common-util.hpp"
#include "common/row-op-util.hpp"
#include "encoder-decoder.hpp"
#include "clientlib.hpp"

ClientLib *client_lib = NULL;

ClientLib::ClientLib(
        uint process_id, const GeePsConfig& config) :
            process_id(process_id), config(config){
  /* Takes in configs */
  host_list = config.host_list;
  num_channels = config.num_comm_channels;
  tcp_base_port = config.tcp_base_port;
  num_servers = config.host_list.size();

  proc_stats.local_opt = config.local_opt;

  /* Init fields */
  CHECK_GE(INITIAL_CLOCK, 0);
  current_iteration = INITIAL_CLOCK;
  fast_clock = INITIAL_CLOCK;
  nr_threads = 0;

  /* Init the tablet server and communication modules */
  CHECK_GT(num_channels, 0);
  comm_channels.resize(num_channels);
  for (uint i = 0; i < num_channels; i++) {
    init_comm_channel(i, config);
  }
}

void ClientLib::init_comm_channel(
        uint channel_id,
        const GeePsConfig& config) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  comm_channel.mutex = make_shared<boost::mutex>();
  comm_channel.cvar = make_shared<boost::condition_variable>();

  /* Init cuda stream and cublas handle */
  cudaStream_t& cuda_stream_recv = comm_channel.cuda_stream_recv;
  cublasHandle_t& cublas_handle_recv = comm_channel.cublas_handle_recv;
  cudaStream_t& cuda_stream_send = comm_channel.cuda_stream_send;
  cublasHandle_t& cublas_handle_send = comm_channel.cublas_handle_send;
  CUDA_CHECK(cudaStreamCreateWithFlags(
      &cuda_stream_recv, cudaStreamNonBlocking));
  CUBLAS_CHECK(cublasCreate(&cublas_handle_recv));
  CUBLAS_CHECK(cublasSetStream(cublas_handle_recv, cuda_stream_recv));
  CUDA_CHECK(cudaStreamCreateWithFlags(
      &cuda_stream_send, cudaStreamNonBlocking));
  CUBLAS_CHECK(cublasCreate(&cublas_handle_send));
  CUBLAS_CHECK(cublasSetStream(cublas_handle_send, cuda_stream_send));

  /* Init communication */
  comm_channel.zmq_ctx = make_shared<zmq::context_t>(1);

  ServerThreadEntry server_entry(
      channel_id, num_channels, process_id, num_servers,
      comm_channel.zmq_ctx, config);
  comm_channel.server_thread = make_shared<boost::thread>(server_entry);

  string client_name = (format("client-%i") % process_id).str();
  vector<string> bind_list;   /* Empty bind_list vector */
  vector<string> connect_list;
  for (uint i = 0; i < host_list.size(); i ++) {
    uint port =
      channel_id + ((port_list.size() != 0) ? port_list[i] : tcp_base_port);
    string connect_endpoint =
        "tcp://" + host_list[i] + ":" + boost::lexical_cast<std::string>(port);
    connect_list.push_back(connect_endpoint);
  }
  comm_channel.router = make_shared<RouterHandler>(
      channel_id, comm_channel.zmq_ctx, connect_list, bind_list,
      client_name, config);
  comm_channel.encoder = make_shared<ClientServerEncode>(
      comm_channel.router, host_list.size(), process_id, config);

  bool work_in_background = true;
  comm_channel.decoder = make_shared<ServerClientDecode>(
      channel_id, comm_channel.zmq_ctx,
      this, work_in_background, config);
  comm_channel.router->start_handler_thread(
      comm_channel.decoder->get_recv_callback());

  /* Start background worker thread */
  string endpoint = "inproc://bg-worker";
  shared_ptr<WorkPuller> work_puller =
      make_shared<WorkPuller>(comm_channel.zmq_ctx, endpoint);
  BackgroundWorker::WorkerCallback iterate_callback =
      bind(&ClientLib::cbk_iterate, this, channel_id, _1);
  BackgroundWorker bg_worker(work_puller);
  bg_worker.add_callback(ITERATE_CMD, iterate_callback);
  comm_channel.bg_worker_thread = make_shared<boost::thread>(bg_worker);

  /* Init work pusher */
  comm_channel.work_pusher = make_shared<WorkPusher>(
                comm_channel.zmq_ctx, endpoint);

  /* Init tables */
  CHECK(config.num_tables);
  comm_channel.cached_tables.resize(config.num_tables);
  for (uint table_id = 0; table_id < config.num_tables; table_id++) {
    CachedTable& cached_table = comm_channel.cached_tables[table_id];
    cached_table.server_clock_min = INITIAL_DATA_AGE;
    cached_table.per_server_data_age.resize(num_servers);
    cached_table.server_clock.resize(num_servers);
    for (uint server_id = 0; server_id < num_servers; server_id++) {
      cached_table.per_server_data_age[server_id] = INITIAL_DATA_AGE;
      cached_table.server_clock[server_id] = INITIAL_DATA_AGE;
    }
    cached_table.data_age = INITIAL_DATA_AGE;
  }
}

void ClientLib::thread_start() {
  unique_lock<mutex> lock(global_mutex);

  thread_data.reset(new ThreadData);
  ThreadData& thread_data_ref = *thread_data;

  /* Set up thread number */
  uint thread_id = nr_threads;
  thread_data_ref.thread_id = thread_id;
  nr_threads++;
  proc_stats.nr_threads++;

  /* Init clocks */
  thread_data_ref.iteration = INITIAL_CLOCK;

  /* Init cuda stream and cublas handle */
  CUDA_CHECK(cudaStreamCreateWithFlags(
      &thread_data_ref.cuda_stream, cudaStreamNonBlocking));
  cublasCreate(&thread_data_ref.cublas_handle);
  cublasSetStream(thread_data_ref.cublas_handle, thread_data_ref.cuda_stream);

  /* Init other fields */
  thread_data_ref.bg_worker_started = false;
}

void ClientLib::shutdown() {
  /* TODO: join the background threads */
  for (uint i = 0; i < num_channels; i++) {
    CommunicationChannel& comm_channel = comm_channels[i];
    /* Shut down background worker thread */
    comm_channel.work_pusher->push_work(BackgroundWorker::STOP_CMD);
    (*comm_channel.bg_worker_thread).join();

    /* Shut down router thread */
    comm_channel.router->stop_handler_thread();

    /* Shut down decoder thread */
    comm_channel.decoder->stop_decoder();
  }
}

void ClientLib::thread_stop() {
  /* Clean up itself */
  // ThreadData& thread_data_ref = *thread_data;
  // (*thread_data_ref.bg_worker_thread).join();
  // CUDA_CHECK(cudaFreeHost(thread_data_ref.cpu_buffer));
  // thread_data.release();

  unique_lock<mutex> global_lock(global_mutex);
  nr_threads--;
}

iter_t ClientLib::get_clock() {
  return thread_data->iteration;
}

uint ClientLib::get_channel_id(table_id_t table, row_idx_t row) {
  uint table_id = table;
  return row / rows_per_channel[table_id];
}

uint ClientLib::get_channel_id(const TableRow& table_row) {
  uint table_id = table_row.first;
  return table_row.second / rows_per_channel[table_id];
}

string ClientLib::json_stats() {
  BgthreadStats bgthread_stats;
  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {
    bgthread_stats += comm_channels[channel_id].bgthread_stats;
  }
  bgthread_stats /= num_channels;
  /* TODO: look at the router stats of all communication channels */
  CommunicationChannel& comm_channel = comm_channels[0];
  proc_stats.router_stats = comm_channel.router->get_stats();
  proc_stats.bgthread_stats = bgthread_stats.to_json();

  /* Get server stat */
  unique_lock<mutex> global_lock(global_mutex);
  bool call_server = true;
  proc_stats.server_stats_refreshed = false;
  while (!proc_stats.server_stats_refreshed) {
    if (call_server) {
      call_server = false;
      global_lock.unlock();  /* Release the lock while sending messages */
      comm_channel.encoder->get_stats(process_id);
      global_lock.lock();
    } else {
      global_cvar.wait(global_lock);  /* Wait is notified by get_stats_cbk() */
    }
  }

  string json = proc_stats.to_json();

  std::string out_path = config.output_dir;
  out_path += "/json_stats.";
  out_path += boost::lexical_cast<std::string>(process_id);
  std::ofstream json_out(out_path.c_str(),
                         std::ofstream::out | std::ofstream::app);
  json_out << json << endl;
  json_out.close();

  return json;
}

void ClientLib::create_oplog_entry(
    uint channel_id, iter_t clock, uint table_id, bool gpu) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = gpu ?
      cached_table.param_cache_gpu :
      cached_table.param_cache_cpu;
  Oplog& oplog = param_cache.oplog;
  OpMemBufferPool& opmem_buffer_pool = param_cache.opmem_buffer_pool;
  CHECK_LT(static_cast<uint>(clock), oplog.size());
  FlatOps *new_flatops = opmem_buffer_pool.get();
  CHECK(new_flatops);
  new_flatops->reset();
  oplog[static_cast<uint>(clock)] = new_flatops;
}

void ClientLib::clock_all(iter_t clock) {
  for (uint table_id = 0; table_id < config.num_tables; table_id++) {
    clock_table(clock, table_id);
  }
}

void ClientLib::clock_table(
    iter_t clock, uint table_id) {
  unique_lock<mutex> lock(global_mutex);

  fast_clock = clock;

  current_iteration = clock;

  /* Signal the server that we have finished clock -1 */
  iter_t signalled_clock = clock - 1;
  /* Let the background communication thread send updates to tablet servers */
  for (uint channel_id = 0; channel_id < comm_channels.size(); channel_id++) {
    push_clock_work(channel_id, signalled_clock, table_id);
  }
}

void ClientLib::push_clock_work(
    uint channel_id, iter_t clock, uint table_id) {
  vector<ZmqPortableBytes> msgs;
  msgs.resize(1);
  msgs[0].init_size(sizeof(bgcomm_clock_msg_t));
  bgcomm_clock_msg_t *clock_msg =
    reinterpret_cast<bgcomm_clock_msg_t *>(msgs[0].data());
  clock_msg->clock = clock;
  clock_msg->table_id = table_id;
  comm_channels[channel_id].work_pusher->push_work(ITERATE_CMD, msgs);
}

void ClientLib::push_updates(
    uint channel_id, iter_t clock, uint table_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  /* There's only one push_updates() thread for each channel,
   * so we don't need to grab the channel lock */
  /* This implementation only works for one thread per machine.
   * When there are multiple threads, their CLOCK messages
   * might be out of order.
   * In that case, we should have a while loop to check the
   * correct clock to send.
   * Sometimes we might need to send multiple CLOCK messages to server
   * on one (in-process) CLOCK push work message. */
  BgthreadStats& bgthread_stats = comm_channel.bgthread_stats;
  push_updates_param_cache(
      channel_id, clock, table_id);
  bgthread_stats.push_updates_iter = clock - 1;
}

void ClientLib::cbk_iterate(
        uint channel_id, vector<ZmqPortableBytes>& args) {
  CHECK_EQ(args.size(), 1);
  bgcomm_clock_msg_t *clock_msg =
    reinterpret_cast<bgcomm_clock_msg_t *>(args[0].data());
  push_updates(channel_id, clock_msg->clock, clock_msg->table_id);
  for (uint i = 0; i < args.size(); i++) {
    args[i].close();
  }
}

iter_t ClientLib::get_iteration() {
  return thread_data->iteration;
}

bool ClientLib::read_batch(
    RowData **buffer_mem_ret, int handle, bool stat) {
  // cout << "APP READ, handle #" << handle << endl;
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.iteration;
  OpSeq& opseq = thread_data_ref.opseq;
  uint op_id = static_cast<uint>(handle);
  CHECK_LT(op_id, opseq.size());
  OpInfo& op_info = opseq[op_id];
  CHECK_EQ(op_info.type, OpInfo::READ);
  CHECK_GE(clock, 0);
  CHECK(op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      op_info.op_data_buffers[op_data_buffers_index];
  if (thread_data_ref.bg_worker_started) {
    tbb::tick_count time_start = tbb::tick_count::now();
    CHECK_EQ(handle, thread_data_ref.last_handle + 1)
        << "handle mismatch in read_batch()";
    thread_data_ref.last_handle = handle;
    unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
    while (op_data_buffer.buffer == NULL) {
      // op_data_buffer.cvar.wait(buffer_lock);
      if (!op_data_buffer.cvar.timed_wait(buffer_lock,
          boost::posix_time::milliseconds(12000))) {
         cerr << "read_row_batch waiting timed out for op #" << op_id
              << ", clock = " << clock << endl;
      }
    }
    CHECK(op_data_buffer.buffer);
    CHECK(!op_data_buffer.buffer_being_used);
    CHECK(!op_data_buffer.updates_ready);
    *buffer_mem_ret = op_data_buffer.buffer;
    op_data_buffer.buffer_being_used = true;
    proc_stats.app_read_time +=
        (tbb::tick_count::now() - time_start).seconds();
  } else {
    ThreadCache& thread_cache = thread_data_ref.thread_cache;
    cudaStream_t& cuda_stream = thread_data_ref.cuda_stream;
    RowData *cpu_buffer = thread_data_ref.cpu_buffer;
    iter_t clock = thread_data_ref.iteration;
    if (!op_info.local) {
      uint num_rows = op_info.rows.size();
      op_data_buffer.buffer = thread_cache.get(num_rows, true);
      iter_t data_age = clock - op_info.slack - 1;
      bool stat = true;
      bool timing = true;
      read_row_batch_param_cache(
          op_data_buffer.buffer, op_info, data_age,
          cuda_stream, cpu_buffer, stat, timing);
      *buffer_mem_ret = op_data_buffer.buffer;
    } else {
      /* Local access */
      if (op_info.data_location == DataStorage::GPU) {
        /* Data in GPU memory */
        op_data_buffer.buffer = op_info.local_storage_ptr;
        CHECK(op_data_buffer.buffer);
      } else {
        /* Data in CPU memory */
        CHECK(op_info.shared_buffer);
        SharedBuffer& shared_buffer = *op_info.shared_buffer;
        uint num_rows = op_info.rows.size();
        unique_lock<mutex> shared_buffer_lock(shared_buffer.mutex);
        if (shared_buffer.buffer) {
          /* Buffer already exists */
          CHECK_GT(shared_buffer.ref_count, 0);
          shared_buffer.ref_count++;
          op_data_buffer.buffer = shared_buffer.buffer;
          CHECK_EQ(shared_buffer.num_rows, num_rows);
        } else {
          CHECK_EQ(shared_buffer.ref_count, 0);
          uint num_rows = op_info.rows.size();
          bool block_waiting = true;
          op_data_buffer.buffer =
              thread_cache.get(num_rows, block_waiting);
          CHECK(op_data_buffer.buffer);
          shared_buffer.buffer = op_data_buffer.buffer;
          shared_buffer.num_rows = num_rows;
          shared_buffer.ref_count++;
          /* Copy CPU local storage to GPU buffer if the app needs to
           * read the data */
          if (op_info.fetch_local) {
            read_batch_local(
                op_data_buffer.buffer, op_info, cuda_stream, cpu_buffer);
          }
        }
        shared_buffer.cvar.notify_all();
      }
      *buffer_mem_ret = op_data_buffer.buffer;
    }
  }
  CHECK(*buffer_mem_ret);
  return true;
}

void ClientLib::postread_batch(int handle) {
  // cout << "APP POSTREAD, handle #" << handle << endl;
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.iteration;
  OpSeq& opseq = thread_data_ref.opseq;
  uint op_id = static_cast<uint>(handle);
  CHECK_LT(op_id, opseq.size());
  OpInfo& op_info = opseq[op_id];
  CHECK_EQ(op_info.type, OpInfo::POST_READ);
  CHECK_GE(op_info.prestep_handle, 0);
  uint prestep_op_id = static_cast<uint>(op_info.prestep_handle);
  CHECK_LT(prestep_op_id, opseq.size());
  OpInfo& prestep_op_info = opseq[prestep_op_id];
  CHECK_EQ(prestep_op_info.type, OpInfo::READ);
  CHECK_GE(clock, 0);
  CHECK(prestep_op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % prestep_op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      prestep_op_info.op_data_buffers[op_data_buffers_index];
  if (thread_data_ref.bg_worker_started) {
    tbb::tick_count time_start = tbb::tick_count::now();
    CHECK_EQ(handle, thread_data_ref.last_handle + 1)
        << "handle mismatch in update_batch()";
    thread_data_ref.last_handle = handle;
    unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
    op_data_buffer.buffer_being_used = false;
    op_data_buffer.updates_ready = true;
    /* Notify the background thread to reclaim the read buffer */
    op_data_buffer.cvar.notify_all();
    proc_stats.app_postread_time +=
        (tbb::tick_count::now() - time_start).seconds();
  } else {
    ThreadCache& thread_cache = thread_data_ref.thread_cache;
    cudaStream_t& cuda_stream = thread_data_ref.cuda_stream;
    RowData *cpu_buffer = thread_data_ref.cpu_buffer;
    if (!op_info.local) {
      CHECK(op_data_buffer.buffer);
      uint num_rows = prestep_op_info.rows.size();
      thread_cache.put(op_data_buffer.buffer, num_rows);
      op_data_buffer.buffer = NULL;
    } else {
      /* Local post-access */
      if (prestep_op_info.data_location != DataStorage::GPU) {
        /* Release cache */
        CHECK(prestep_op_info.shared_buffer);
        SharedBuffer& shared_buffer = *prestep_op_info.shared_buffer;
        unique_lock<mutex> shared_buffer_lock(shared_buffer.mutex);
        CHECK_EQ(shared_buffer.buffer, op_data_buffer.buffer);
        CHECK_GT(shared_buffer.ref_count, 0);
        shared_buffer.ref_count--;
        /* The last access decides whether it needs keep or not */
        shared_buffer.need_keep = op_info.keep_local;
        if (shared_buffer.ref_count == 0) {
          /* No more references, write the changes back
           * to the CPU local storage and release buffer */
          /* Copy GPU buffer to CPU local storage if the app has
           * modifications */
          uint num_rows = prestep_op_info.rows.size();
          CHECK_EQ(shared_buffer.num_rows, num_rows);
          if (shared_buffer.need_keep) {
            update_batch_local(
                prestep_op_info, op_data_buffer.buffer,
                cuda_stream, cpu_buffer);
          }
          thread_cache.put(op_data_buffer.buffer, num_rows);
          shared_buffer.buffer = NULL;
        }
        shared_buffer.cvar.notify_all();
      }
      op_data_buffer.buffer = NULL;
    }
    CHECK(!op_data_buffer.buffer);
    CHECK(!op_data_buffer.buffer_being_used);
  }
}

void ClientLib::preupdate_batch(
    RowOpVal **updates_mem_ret, int handle, bool stat) {
  // cout << "APP PRE_WRITE, handle #" << handle << endl;
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.iteration;
  OpSeq& opseq = thread_data_ref.opseq;
  uint op_id = static_cast<uint>(handle);
  CHECK_LT(op_id, opseq.size());
  OpInfo& op_info = opseq[op_id];
  CHECK_EQ(op_info.type, OpInfo::PRE_WRITE) << " op_id = " << op_id;
  CHECK_GE(clock, 0);
  CHECK(op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      op_info.op_data_buffers[op_data_buffers_index];
  if (thread_data_ref.bg_worker_started) {
    tbb::tick_count time_start = tbb::tick_count::now();
    CHECK_EQ(handle, thread_data_ref.last_handle + 1)
        << "handle mismatch in preupdate_batch()";
    thread_data_ref.last_handle = handle;
    unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
    while (op_data_buffer.buffer == NULL) {
      // op_data_buffer.cvar.wait(buffer_lock);
      if (!op_data_buffer.cvar.timed_wait(buffer_lock,
          boost::posix_time::milliseconds(12000))) {
         cerr << "update_batch waiting timed out for op #" << op_id
              << ", clock = " << clock << endl;
      }
    }
    CHECK(op_data_buffer.buffer);
    CHECK(!op_data_buffer.buffer_being_used);
    CHECK(!op_data_buffer.updates_ready);
    *updates_mem_ret = op_data_buffer.buffer;
    op_data_buffer.buffer_being_used = true;
    proc_stats.app_prewrite_time +=
        (tbb::tick_count::now() - time_start).seconds();
  } else {
    uint num_rows = op_info.rows.size();
    op_data_buffer.buffer = thread_data_ref.thread_cache.get(num_rows, true);
    *updates_mem_ret = op_data_buffer.buffer;
  }
  CHECK(*updates_mem_ret);
}

void ClientLib::update_batch(int handle) {
  // cout << "APP WRITE, handle #" << handle << endl;
  ThreadData& thread_data_ref = *thread_data;
  iter_t clock = thread_data_ref.iteration;
  OpSeq& opseq = thread_data_ref.opseq;
  uint op_id = static_cast<uint>(handle);
  CHECK_LT(op_id, opseq.size());
  OpInfo& op_info = opseq[op_id];
  CHECK_EQ(op_info.type, OpInfo::WRITE);
  CHECK_GE(op_info.prestep_handle, 0);
  uint prestep_op_id = static_cast<uint>(op_info.prestep_handle);
  CHECK_LT(prestep_op_id, opseq.size());
  OpInfo& prestep_op_info = opseq[prestep_op_id];
  CHECK_EQ(prestep_op_info.type, OpInfo::PRE_WRITE);
  CHECK_GE(clock, 0);
  CHECK(prestep_op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % prestep_op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      prestep_op_info.op_data_buffers[op_data_buffers_index];
  if (thread_data_ref.bg_worker_started) {
    tbb::tick_count time_start = tbb::tick_count::now();
    CHECK_EQ(handle, thread_data_ref.last_handle + 1)
        << "handle mismatch in update_batch()";
    thread_data_ref.last_handle = handle;
    unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
    op_data_buffer.buffer_being_used = false;
    op_data_buffer.updates_ready = true;
    /* Notify the background thread to apply the updates */
    op_data_buffer.cvar.notify_all();
    proc_stats.app_write_time +=
        (tbb::tick_count::now() - time_start).seconds();
  } else {
    /* Apply updates */
    CHECK(op_data_buffer.buffer);
    cudaStream_t& cuda_stream = thread_data_ref.cuda_stream;
    RowData *cpu_buffer = thread_data_ref.cpu_buffer;
    CHECK(!prestep_op_info.local);
    bool stat = true;
    bool timing = true;
    update_batch_param_cache(
        prestep_op_info, op_data_buffer.buffer, clock,
        cuda_stream, cpu_buffer, stat, timing);
    uint num_rows = prestep_op_info.rows.size();
    thread_data_ref.thread_cache.put(op_data_buffer.buffer, num_rows);
    op_data_buffer.buffer = NULL;
  }
}

void ClientLib::iterate() {
  ThreadData& thread_data_ref = *thread_data;
  thread_data_ref.iteration++;
  if (thread_data_ref.bg_worker_started) {
    /* Do nothing */
    thread_data_ref.last_handle = -1;
  } else {
    clock_all(thread_data_ref.iteration);
  }
}
