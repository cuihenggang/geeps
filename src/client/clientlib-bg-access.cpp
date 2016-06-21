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

void ClientLib::start_opseq() {
  /* Create a background worker thread for read/write.
   * The background worker thread will do this operation
   * starting from this clock. */
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;
  iter_t current_clock = thread_data_ref.iteration;
  for (uint i = 0; i < opseq.size(); i++) {
    opseq[i].last_finished_clock = current_clock - 1;
  }

  /* Free its CPU buffer */
  CUDA_CHECK(cudaFreeHost(thread_data_ref.cpu_buffer));

  /* Start alloc worker */
  OpSeqWorkerInfo& alloc_worker_info = thread_data_ref.alloc_worker_info;
  alloc_worker_info.opseq_ptr = &opseq;
  alloc_worker_info.thread_cache_ptr = &thread_data_ref.thread_cache;
  alloc_worker_info.cpu_buffer_size = thread_data_ref.cpu_buffer_size;
  thread_data_ref.alloc_worker_thread = make_shared<boost::thread>(bind(
      &ClientLib::alloc_worker_entry, this, alloc_worker_info));
  /* Start reclaim worker */
  OpSeqWorkerInfo& reclaim_worker_info = thread_data_ref.reclaim_worker_info;
  reclaim_worker_info.opseq_ptr = &opseq;
  reclaim_worker_info.thread_cache_ptr = &thread_data_ref.thread_cache;
  reclaim_worker_info.cpu_buffer_size = thread_data_ref.cpu_buffer_size;
  thread_data_ref.alloc_worker_thread = make_shared<boost::thread>(bind(
      &ClientLib::reclaim_worker_entry, this, reclaim_worker_info));
  thread_data_ref.bg_worker_started = true;
  thread_data_ref.last_handle = -1;
}

void ClientLib::alloc_worker_entry(OpSeqWorkerInfo& worker_info) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  RowData *cpu_buffer;
  /* On CUDA 7.5 and CUDA 7.0, the cudaMallocHost() will sometimes fail
   * even though there is still available memory to allocate.
   * I don't know why it's happening, but as a workaround,
   * I added a while loop to retry cudaMallocHost(). */
  // CUDA_CHECK(cudaMallocHost(&cpu_buffer, worker_info.cpu_buffer_size));
  while (cudaMallocHost(
      &cpu_buffer, worker_info.cpu_buffer_size) != cudaSuccess) {
    cout << "*** WARNING: cudaMallocHost failed at process " << process_id
         << ", will retry"
         << endl;
  }
  cudaStream_t cuda_stream;
  cublasHandle_t cublas_handle;
  CUDA_CHECK(cudaStreamCreateWithFlags(&cuda_stream, cudaStreamNonBlocking));
  CUBLAS_CHECK(cublasCreate(&cublas_handle));
  CUBLAS_CHECK(cublasSetStream(cublas_handle, cuda_stream));
  tbb::tick_count tick_start;
  while (true) {
    for (uint i = 0; i < opseq.size(); i++) {
      OpInfo& op_info = opseq[i];
      if (op_info.type == OpInfo::READ && op_info.local) {
        // cout << "BG LocalAccess, handle #" << i << endl;
        alloc_worker_localaccess(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::READ && !op_info.local) {
        // cout << "BG Read, handle #" << i << endl;
        alloc_worker_read(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::PRE_WRITE) {
        // cout << "BG PreUpdate, handle #" << i << endl;
        alloc_worker_preupdate(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::CLOCK) {
        // cout << "BG Clock, handle #" << i;
        /* Do not need to wait.
         * Because we have a set of OpDataBuffer for every clock,
         * the alloc_worker can start the next clock before
         * reclaim_worker finishes */
        break;
        /* Break out of the for loop because the operations after CLOCK
         * are unrepeated ones */
      }
    }
  }
  /* TODO: free CPU buffer */
  /* TODO: destroy stream and handle */
}

void ClientLib::reclaim_worker_entry(OpSeqWorkerInfo& worker_info) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  RowData *cpu_buffer;
  /* On CUDA 7.5 and CUDA 7.0, the cudaMallocHost() will sometimes fail
   * even though there is still available memory to allocate.
   * I don't know why it's happening, but as a workaround,
   * I added a while loop to retry cudaMallocHost(). */
  // CUDA_CHECK(cudaMallocHost(&cpu_buffer, worker_info.cpu_buffer_size));
  while (cudaMallocHost(
      &cpu_buffer, worker_info.cpu_buffer_size) != cudaSuccess) {
    cout << "*** WARNING: cudaMallocHost failed at process " << process_id
         << ", will retry"
         << endl;
  }
  cudaStream_t cuda_stream;
  cublasHandle_t cublas_handle;
  CUDA_CHECK(cudaStreamCreateWithFlags(&cuda_stream, cudaStreamNonBlocking));
  CUBLAS_CHECK(cublasCreate(&cublas_handle));
  CUBLAS_CHECK(cublasSetStream(cublas_handle, cuda_stream));
  while (true) {
    for (uint i = 0; i < opseq.size(); i++) {
      OpInfo& op_info = opseq[i];
      if (op_info.type == OpInfo::POST_READ && op_info.local) {
        // cout << "BG PostLocalAccess, handle #" << i << endl;
        reclaim_worker_postlocalaccess(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::POST_READ && !op_info.local) {
        // cout << "BG PostRead, handle #" << i << endl;
        reclaim_worker_postread(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::WRITE) {
        // cout << "BG Update, handle #" << i << endl;
        reclaim_worker_update(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        continue;
      }
      if (op_info.type == OpInfo::CLOCK) {
        // cout << "BG Clock, handle #" << i;
        reclaim_worker_clock(
            op_info, worker_info, cpu_buffer, cuda_stream, cublas_handle);
        break;
        /* Break out of the loop because the operations after CLOCK
         * are unrepeated ones */
      }
    }
  }
  /* TODO: free CPU buffer */
  /* TODO: destroy stream and handle */
}

void ClientLib::alloc_worker_localaccess(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;
  CHECK_GE(clock, 0);

  CHECK(op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      op_info.op_data_buffers[op_data_buffers_index];
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  CHECK(op_data_buffer.buffer == NULL);
  CHECK(!op_data_buffer.buffer_being_used);
  CHECK(!op_data_buffer.updates_ready);
  if (op_info.data_location == DataStorage::GPU) {
    /* Because the data is pinned in GPU memory,
     * we can just use it, without allocation or copying */
    op_data_buffer.buffer = op_info.local_storage_ptr;
    CHECK(op_data_buffer.buffer);
    op_data_buffer.buffer_being_used = false;
    op_data_buffer.updates_ready = false;
    op_info.last_finished_clock = clock;
    op_data_buffer.cvar.notify_all();
    proc_stats.bg_read_time +=
        (tbb::tick_count::now() - time_start).seconds();
    return;
  }
  /* In case of data in CPU memory */
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
    tick_start = tbb::tick_count::now();
    while ((op_data_buffer.buffer = thread_cache.get(num_rows, true, clock)) == NULL) {
      cerr << "opseq worker of machine " << process_id
            << " has no more space, op #" //<< i
            << endl;
       cerr << "clock = " << clock << endl;
    }
    op_info.alloc_wait_time +=
          (tbb::tick_count::now() - tick_start).seconds();
    CHECK(op_data_buffer.buffer);
    shared_buffer.buffer = op_data_buffer.buffer;
    shared_buffer.num_rows = num_rows;
    shared_buffer.ref_count++;
    /* Copy CPU local storage to GPU buffer if the app needs to
     * read the data */
    if (op_info.fetch_local) {
      tick_start = tbb::tick_count::now();
      read_batch_local(
          op_data_buffer.buffer, op_info, cuda_stream, cpu_buffer);
      op_info.fetch_time +=
          (tbb::tick_count::now() - tick_start).seconds();
      op_info.rows_fetched += num_rows;
    }
  }
  shared_buffer.cvar.notify_all();
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  op_data_buffer.cvar.notify_all();
  proc_stats.bg_read_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::alloc_worker_read(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;

  CHECK(op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      op_info.op_data_buffers[op_data_buffers_index];
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  CHECK(op_data_buffer.buffer == NULL);
  CHECK(!op_data_buffer.buffer_being_used);
  CHECK(!op_data_buffer.updates_ready);
  uint num_rows = op_info.rows.size();
  tick_start = tbb::tick_count::now();
  // op_data_buffer.buffer = thread_cache.get(num_rows, /* wait for space */ true);
  while ((op_data_buffer.buffer = thread_cache.get(num_rows, true, clock)) == NULL) {
    cerr << "opseq worker of machine " << process_id
          << " has no more space, op #" //<< i
          << endl;
    cerr << "clock = " << clock << endl;
  }
  op_info.alloc_wait_time +=
        (tbb::tick_count::now() - tick_start).seconds();
  CHECK(op_data_buffer.buffer);
  iter_t data_age = clock - op_info.slack - 1;
  tick_start = tbb::tick_count::now();
  read_row_batch_param_cache(
      op_data_buffer.buffer, op_info, data_age,
      cuda_stream, cpu_buffer, true, true);
  op_info.read_time +=
      (tbb::tick_count::now() - tick_start).seconds();
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  op_data_buffer.cvar.notify_all();
  proc_stats.bg_read_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::alloc_worker_preupdate(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;
  CHECK_GE(clock, 0);
  CHECK(op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      op_info.op_data_buffers[op_data_buffers_index];
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  CHECK(op_data_buffer.buffer == NULL);
  CHECK(!op_data_buffer.buffer_being_used);
  CHECK(!op_data_buffer.updates_ready);
  tick_start = tbb::tick_count::now();
  uint num_rows = op_info.rows.size();
  // op_data_buffer.buffer = thread_cache.get(num_rows, /* wait for space */ true);
  while ((op_data_buffer.buffer = thread_cache.get(num_rows, true, clock)) == NULL) {
     cerr << "opseq worker of machine " << process_id
          << " has no more space, op #" //<< i
          << endl;
     cerr << "clock = " << clock << endl;
  }
  op_info.alloc_wait_time +=
        (tbb::tick_count::now() - tick_start).seconds();
  CHECK(op_data_buffer.buffer);
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  op_data_buffer.cvar.notify_all();
  proc_stats.bg_prewrite_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::reclaim_worker_postlocalaccess(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;
  CHECK_GE(op_info.prestep_handle, 0);
  uint prestep_op_id = static_cast<uint>(op_info.prestep_handle);
  CHECK_LT(prestep_op_id, opseq.size());
  OpInfo& prestep_op_info = opseq[prestep_op_id];
  CHECK_EQ(prestep_op_info.type, OpInfo::READ);

  CHECK(prestep_op_info.op_data_buffers.size());
  size_t op_data_buffers_index =
      static_cast<size_t>(clock) % prestep_op_info.op_data_buffers.size();
  OpDataBuffer& op_data_buffer =
      prestep_op_info.op_data_buffers[op_data_buffers_index];
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  CHECK(prestep_op_info.local);
  /* Wait for the application to finish using the read buffer */
  tick_start = tbb::tick_count::now();
  while (!op_data_buffer.updates_ready) {
    // op_data_buffer.cvar.wait(buffer_lock);
    if (!op_data_buffer.cvar.timed_wait(buffer_lock,
        boost::posix_time::milliseconds(12000))) {
       cerr << "opseq worker of machine " << process_id
            << " waiting for updates ready timed out, op #"
            << prestep_op_id << endl;
       // cerr << "POST_READ op #" << i << endl;
       cerr << "clock = " << clock << endl;
       cerr << "preop.updates_ready = " << op_data_buffer.updates_ready << endl;
       cerr << "preop.buffer = " << op_data_buffer.buffer << endl;
       cerr << "preop.buffer_being_used = " << op_data_buffer.buffer_being_used << endl;
    }
  }
  op_info.reclaim_wait_time +=
      (tbb::tick_count::now() - tick_start).seconds();
  CHECK(op_data_buffer.updates_ready);
  CHECK(op_data_buffer.buffer);
  CHECK(!op_data_buffer.buffer_being_used);
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
        tick_start = tbb::tick_count::now();
        update_batch_local(
            prestep_op_info, op_data_buffer.buffer, cuda_stream, cpu_buffer);
        op_info.keep_time +=
            (tbb::tick_count::now() - tick_start).seconds();
        op_info.rows_kept += num_rows;
      }
      thread_cache.put(op_data_buffer.buffer, num_rows);
      shared_buffer.buffer = NULL;
    }
    shared_buffer.cvar.notify_all();
  }
  op_data_buffer.buffer = NULL;
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  /* Don't need to notify, no one is waiting for this */
  proc_stats.bg_postread_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::reclaim_worker_postread(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;
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
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  CHECK(!prestep_op_info.local);
  /* Wait for the application to finish using the read buffer */
  tick_start = tbb::tick_count::now();
  while (!op_data_buffer.updates_ready) {
    // op_data_buffer.cvar.wait(buffer_lock);
    if (!op_data_buffer.cvar.timed_wait(buffer_lock,
        boost::posix_time::milliseconds(12000))) {
       cerr << "opseq worker of machine " << process_id
            << " waiting for updates ready timed out, op #"
            << prestep_op_id << endl;
       // cerr << "POST_READ op #" << i << endl;
       cerr << "clock = " << clock << endl;
       cerr << "preop.last_finished_clock = " << prestep_op_info.last_finished_clock << endl;
       cerr << "preop.updates_ready = " << op_data_buffer.updates_ready << endl;
       cerr << "preop.buffer = " << op_data_buffer.buffer << endl;
       cerr << "preop.buffer_being_used = " << op_data_buffer.buffer_being_used << endl;
    }
  }
  op_info.reclaim_wait_time +=
      (tbb::tick_count::now() - tick_start).seconds();
  CHECK(op_data_buffer.updates_ready);
  CHECK(op_data_buffer.buffer);
  CHECK(!op_data_buffer.buffer_being_used);
  /* Release cache */
  uint num_rows = prestep_op_info.rows.size();
  thread_cache.put(op_data_buffer.buffer, num_rows);
  op_data_buffer.buffer = NULL;
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  /* Don't need to notify, no one is waiting for this */
  proc_stats.bg_postread_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::reclaim_worker_update(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  ThreadCache& thread_cache = *worker_info.thread_cache_ptr;

  tbb::tick_count tick_start;
  tbb::tick_count time_start = tbb::tick_count::now();
  iter_t clock = op_info.last_finished_clock + 1;
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
  unique_lock<mutex> buffer_lock(op_data_buffer.mutex);
  /* Wait for the updates to be ready */
  tick_start = tbb::tick_count::now();
  while (!op_data_buffer.updates_ready) {
    // op_data_buffer.cvar.wait(buffer_lock);
    if (!op_data_buffer.cvar.timed_wait(buffer_lock,
        boost::posix_time::milliseconds(12000))) {
       cerr << "opseq worker of machine " << process_id
            << " waiting for updates ready timed out, op #"
            << prestep_op_id << endl;
       // cerr << "WRITE op #" << i << endl;
       cerr << "clock = " << clock << endl;
       cerr << "last_finished_clock = " << op_info.last_finished_clock << endl;
       cerr << "preop.last_finished_clock = " << prestep_op_info.last_finished_clock << endl;
       cerr << "preop.updates_ready = " << op_data_buffer.updates_ready << endl;
       cerr << "preop.buffer = " << op_data_buffer.buffer << endl;
       cerr << "preop.buffer_being_used = " << op_data_buffer.buffer_being_used << endl;
    }
    op_info.reclaim_wait_time +=
        (tbb::tick_count::now() - tick_start).seconds();
  }
  CHECK(op_data_buffer.updates_ready);
  CHECK(op_data_buffer.buffer);
  CHECK(!op_data_buffer.buffer_being_used);
  /* Apply updates */
  CHECK(!prestep_op_info.local);
  tick_start = tbb::tick_count::now();
  update_batch_param_cache(
      prestep_op_info, op_data_buffer.buffer, clock,
      cuda_stream, cpu_buffer, true, true);
  op_info.write_time +=
      (tbb::tick_count::now() - tick_start).seconds();
  /* Release cache */
  uint num_rows = prestep_op_info.rows.size();
  thread_cache.put(op_data_buffer.buffer, num_rows);
  op_data_buffer.buffer = NULL;
  op_data_buffer.buffer_being_used = false;
  op_data_buffer.updates_ready = false;
  op_info.last_finished_clock = clock;
  /* Signal a table_clock if that's the last write of this table */
  if (op_info.table_last_write) {
    uint table_id = op_info.table_id;
    /* Signalling a "clock + 1" means that we have finished "clock" */
    clock_table(clock + 1, table_id);
  }
  /* Don't need to notify, no one is waiting for this */
  proc_stats.bg_write_time +=
      (tbb::tick_count::now() - time_start).seconds();
}

void ClientLib::reclaim_worker_clock(
    OpInfo& op_info, OpSeqWorkerInfo& worker_info, RowData *cpu_buffer,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle) {
  OpSeq& opseq = *worker_info.opseq_ptr;
  iter_t clock = op_info.last_finished_clock + 1;
  op_info.last_finished_clock = clock;
  if (config.log_interval > 0 &&
      clock > 0 && clock % config.log_interval == 0) {
    cout << "bg_worker_times:" << endl;
    size_t total_fetch = 0;
    size_t total_keep = 0;
    for (uint i = 0; i < opseq.size(); i++) {
      OpInfo& op_info = opseq[i];
      total_fetch += op_info.rows_fetched;
      total_keep += op_info.rows_kept;
      cerr << i
           << "," << op_info.read_time
           << "," << op_info.fetch_time
           << "," << op_info.alloc_wait_time
           << "," << op_info.write_time
           << "," << op_info.keep_time
           << "," << op_info.reclaim_wait_time
           << "," << op_info.rows_fetched
           << "," << op_info.rows_kept
           << endl;
    }
    cerr << "total_fetch=" << total_fetch << endl;
    cerr << "total_keep=" << total_keep << endl;
  }
}
