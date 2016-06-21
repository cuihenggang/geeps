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

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;
using boost::bind;
using boost::make_shared;
using boost::shared_ptr;
using boost::unique_lock;
using boost::mutex;
using boost::condition_variable;

void ClientLib::start_gather_info() {
}

void ClientLib::finish_gather_info() {
}

int ClientLib::virtual_read_batch(
    table_id_t table, const vector<row_idx_t>& row_ids, iter_t staleness,
    size_t num_val_limit) {
  OpInfo opinfo(OpInfo::READ, table, row_ids, staleness, num_val_limit);
  return virtual_op(opinfo);
}

int ClientLib::virtual_postread_batch(int prestep_handle) {
  OpInfo opinfo(OpInfo::POST_READ, prestep_handle);
  return virtual_op(opinfo);
}

int ClientLib::virtual_prewrite_batch(
    table_id_t table, const vector<row_idx_t>& row_ids,
    size_t num_val_limit) {
  OpInfo opinfo(OpInfo::PRE_WRITE, table, row_ids, 0, num_val_limit);
  return virtual_op(opinfo);
}

int ClientLib::virtual_write_batch(int prestep_handle) {
  OpInfo opinfo(OpInfo::WRITE, prestep_handle);
  return virtual_op(opinfo);
}

int ClientLib::virtual_localaccess_batch(
    table_id_t table, const vector<row_idx_t>& row_ids,
    size_t num_val_limit, bool fetch) {
  bool local = true;
  OpInfo opinfo(OpInfo::READ, table, row_ids, 0, num_val_limit, local, fetch);
  return virtual_op(opinfo);
}

int ClientLib::virtual_postlocalaccess_batch(
    int prestep_handle, bool keep) {
  bool local = true;
  OpInfo opinfo(OpInfo::POST_READ, prestep_handle, local, keep);
  return virtual_op(opinfo);
}

int ClientLib::virtual_clock() {
  OpInfo opinfo(OpInfo::CLOCK);
  return virtual_op(opinfo);
}

int ClientLib::virtual_op(const OpInfo& opinfo) {
  ThreadData& thread_data_ref = *thread_data;
  /* Record to operation sequence */
  thread_data_ref.opseq.push_back(opinfo);
  return static_cast<int>(thread_data_ref.opseq.size() - 1);
}

void ClientLib::finish_virtual_iteration() {
  /* Summarize information */
  vi_thread_summarize();

  vi_process_finalize();

  /* Make decisions based on the virtual iteration for the current thread */
  vi_thread_finalize();
}

void ClientLib::vi_thread_summarize() {
  /* Create local storage */
  vi_create_local_storage();

  /* Decide parameter cache placement */
  vi_decide_param_cache();

  /* Create thread cache */
  vi_create_thread_cache();
}

struct LocalKeyBatchInfo {
  table_id_t table;
  vector<row_idx_t> rows;
  size_t num_rows;
  size_t num_fetches;
  size_t num_keeps;
  bool gpu;
  size_t key_index;
  SharedBuffer *shared_buffer;
  LocalKeyBatchInfo() :
      num_fetches(0), num_keeps(0), gpu(false), shared_buffer(NULL) {}
};

struct ParamKeyBatchInfo {
  size_t num_rows;
  bool gpu;
  ParamKeyBatchInfo() :
      num_rows(0), gpu(false) {}
};

void ClientLib::vi_create_local_storage() {
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;

  /* Create op_data_buffers */
  size_t op_data_buffer_size = OP_BUFFER_SIZE;
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.type == OpInfo::READ || opinfo.type == OpInfo::PRE_WRITE) {
      opinfo.op_data_buffers.resize(op_data_buffer_size);
    }
  }

  typedef boost::unordered_map<TableRow, LocalKeyBatchInfo> LocalKeyBatchMap;
  LocalKeyBatchMap local_key_batches;
  typedef boost::unordered_map<TableRow, ParamKeyBatchInfo> ParamKeyBatchMap;
  ParamKeyBatchMap param_key_batches;

  ngr_capacity = config.gpu_memory_capacity / sizeof(RowData);
  size_t ngr_used = 0;
  size_t ngr_left = ngr_capacity;
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.local && opinfo.type == OpInfo::READ) {
      CHECK(opinfo.rows.size());
      TableRow first_key(opinfo.table, opinfo.rows[0]);
      if (!local_key_batches.count(first_key)) {
        /* New key batch */
        LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
        key_batch_info.table = opinfo.table;
        key_batch_info.rows = opinfo.rows;
      }
      LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
      CHECK_EQ(key_batch_info.rows.size(), opinfo.rows.size());
      if (opinfo.fetch_local) {
        key_batch_info.num_fetches++;
      }
    }
    if (opinfo.type == OpInfo::POST_READ) {
      int preop_id = opinfo.prestep_handle;
      CHECK_GE(preop_id, 0);
      CHECK_LT(preop_id, opseq.size());
      OpInfo& preopinfo = opseq[preop_id];
      if (preopinfo.local) {
        CHECK(preopinfo.rows.size());
        TableRow first_key(preopinfo.table, preopinfo.rows[0]);
        CHECK(local_key_batches.count(first_key));
        LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
        CHECK_EQ(key_batch_info.rows.size(), preopinfo.rows.size());
        if (opinfo.keep_local) {
          key_batch_info.num_keeps++;
        }
      }
    }
  }

  size_t ngr_needed_for_local_storage = 0;
  size_t ngr_needed_for_fetchkeep_local_storage = 0;
  for (LocalKeyBatchMap::iterator it = local_key_batches.begin();
       it != local_key_batches.end(); it++) {
    LocalKeyBatchInfo& key_batch_info = it->second;
    ngr_needed_for_local_storage += key_batch_info.rows.size();
    if (key_batch_info.num_fetches || key_batch_info.num_keeps) {
      ngr_needed_for_fetchkeep_local_storage += key_batch_info.rows.size();
    }
  }
  // cout << "ngr_needed_for_local_storage = "
       // << ngr_needed_for_local_storage << endl;
  // cout << "ngr_needed_for_fetchkeep_local_storage = "
       // << ngr_needed_for_fetchkeep_local_storage << endl;

  /* Decide GPU local storage rows */
  size_t nr_being_used_peak;
  size_t nr_used_all;
  size_t max_nr_each_access;
  bool all_data_in_gpu = false;
  while (true) {
    size_t ngr_increased = 0;
    /* Calculate the peak size */
    size_t nr_being_used_now = 0;
    nr_being_used_peak = 0;
    nr_used_all = 0;
    max_nr_each_access = 0;
    size_t nr_being_used_second_peak = 0;
    int max_peak_start = -1;
    int this_peak_start = -1;
    int second_peak_start = -1;
    /* Decide peak size */
    for (size_t i = 0; i < opseq.size(); i++) {
      OpInfo& opinfo = opseq[i];
      if (opinfo.type == OpInfo::READ || opinfo.type == OpInfo::PRE_WRITE) {
        if (opinfo.local) {
          CHECK(opinfo.rows.size());
          TableRow first_key(opinfo.table, opinfo.rows[0]);
          CHECK(local_key_batches.count(first_key));
          LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
          if (key_batch_info.gpu) {
            /* Don't count if the local storage is in GPU */
            continue;
          }
        }
        if (this_peak_start == -1) {
          this_peak_start = i;
        }
        nr_being_used_now += opinfo.rows.size();
        nr_used_all += opinfo.rows.size();
        if (opinfo.rows.size() > max_nr_each_access) {
          max_nr_each_access = opinfo.rows.size();
        }
      }
      if (opinfo.type == OpInfo::POST_READ || opinfo.type == OpInfo::WRITE) {
        int preop_id = opinfo.prestep_handle;
        CHECK_GE(preop_id, 0);
        CHECK_LT(preop_id, opseq.size());
        OpInfo& preopinfo = opseq[preop_id];
        if (preopinfo.local) {
          CHECK(preopinfo.rows.size());
          TableRow first_key(preopinfo.table, preopinfo.rows[0]);
          CHECK(local_key_batches.count(first_key));
          LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
          if (key_batch_info.gpu) {
            /* Don't count if the local storage is in GPU */
            continue;
          }
        }
        if (this_peak_start != -1) {
          /* End of a peak */
          if (nr_being_used_now > nr_being_used_peak) {
            nr_being_used_second_peak = nr_being_used_peak;
            second_peak_start = max_peak_start;
            nr_being_used_peak = nr_being_used_now;
            max_peak_start = this_peak_start;
          } else if (nr_being_used_now > nr_being_used_second_peak) {
            nr_being_used_second_peak = nr_being_used_now;
            second_peak_start = this_peak_start;
          }
        }
        nr_being_used_now -= preopinfo.rows.size();
        this_peak_start = -1;
        CHECK_GE(nr_being_used_now, 0);
      }
    }
    CHECK_NE(second_peak_start, second_peak_start + 1);
    // cout << "nr_being_used_peak = " << nr_being_used_peak << endl;
    // cout << "max_peak_start = " << max_peak_start << endl;
    // cout << "nr_being_used_second_peak = " << nr_being_used_second_peak << endl;
    // cout << "second_peak_start = " << second_peak_start << endl;
    if (all_data_in_gpu) {
      /* All data has been assigned to GPU memory */
      break;
    }
    /* Thread cache will be at least twice the size of peak access */
    CHECK_GE(ngr_capacity, nr_being_used_peak * 2);
    if (ngr_needed_for_fetchkeep_local_storage + nr_being_used_peak * 2
        <= ngr_capacity) {
      /* Keep all the fetchkeep local storage in GPU */
      // cout << "Keep all fetchkeep local storage in GPU\n";
      for (LocalKeyBatchMap::iterator it = local_key_batches.begin();
           it != local_key_batches.end(); it++) {
        LocalKeyBatchInfo& key_batch_info = it->second;
        if (key_batch_info.gpu == true) {
          continue;
        }
        if (key_batch_info.num_fetches || key_batch_info.num_keeps) {
          key_batch_info.gpu = true;
          ngr_used += key_batch_info.rows.size();
        }
      }
      all_data_in_gpu = true;
      /* Do not break out of the while(),
       * because we need to recalculate the peak */
    } else {
      /* Keep a part of fetchkeep local storage in GPU */
      /* Try to reduce the peak */
      bool peak_changed = false;
      CHECK_GE(max_peak_start, 0);
      CHECK_LT(max_peak_start, opseq.size());
      for (int i = max_peak_start; i < opseq.size(); i++) {
        OpInfo& opinfo = opseq[i];
        if (opinfo.type == OpInfo::READ || opinfo.type == OpInfo::PRE_WRITE) {
          if (!opinfo.local) {
            continue;
          }
          CHECK(opinfo.rows.size());
          TableRow first_key(opinfo.table, opinfo.rows[0]);
          CHECK(local_key_batches.count(first_key));
          LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
          if (key_batch_info.gpu) {
            /* Don't count if the local storage is in GPU */
            continue;
          }
          if (key_batch_info.num_fetches || key_batch_info.num_keeps) {
            size_t size_after_changing_peak =
                nr_being_used_peak - key_batch_info.rows.size();
            CHECK_GE(size_after_changing_peak, 0);
            if (size_after_changing_peak < nr_being_used_second_peak) {
              size_after_changing_peak = nr_being_used_second_peak;
            }
            if (ngr_used + key_batch_info.rows.size()
                + size_after_changing_peak * 2
                    <= ngr_capacity) {
              key_batch_info.gpu = true;
              ngr_used += key_batch_info.rows.size();
              ngr_increased++;
              peak_changed = true;
              // cout << "peak changed" << endl;
              break;
            }
          }
        }
        if (opinfo.type == OpInfo::POST_READ || opinfo.type == OpInfo::WRITE) {
          /* End of this peak */
          break;
        }
      }
      if (!peak_changed) {
        /* Randomly pick some */
        for (LocalKeyBatchMap::iterator it = local_key_batches.begin();
             it != local_key_batches.end(); it++) {
          LocalKeyBatchInfo& key_batch_info = it->second;
          if (key_batch_info.gpu) {
            continue;
          }
          if (key_batch_info.num_fetches || key_batch_info.num_keeps) {
            if (ngr_used + key_batch_info.rows.size() + nr_being_used_peak * 2
                <= ngr_capacity) {
              key_batch_info.gpu = true;
              ngr_used += key_batch_info.rows.size();
              ngr_increased++;
            }
          }
        }
      }
      if (!ngr_increased) {
        CHECK_LT(config.mm_warning_level, 2);
        break;
      }
    }
  }
  // cout << "nr_being_used_peak = " << nr_being_used_peak << endl;
  thread_data_ref.ngr_used = ngr_used;

  /* Make GPU local storage key list */
  RowKeys& local_row_keys_cpu = thread_data_ref.local_row_keys_cpu;
  RowKeys& local_row_keys_gpu = thread_data_ref.local_row_keys_gpu;
  local_row_keys_gpu.clear();
  local_row_keys_cpu.clear();
  for (LocalKeyBatchMap::iterator it = local_key_batches.begin();
       it != local_key_batches.end(); it++) {
    LocalKeyBatchInfo& key_batch_info = it->second;
    if (key_batch_info.gpu) {
      /* Insert to GPU key list */
      key_batch_info.key_index = local_row_keys_gpu.size();
      for (size_t j = 0; j < key_batch_info.rows.size(); j++) {
        RowKey row_key(key_batch_info.table, key_batch_info.rows[j]);
        local_row_keys_gpu.push_back(row_key);
      }
    } else {
      /* Insert to CPU key list */
      key_batch_info.key_index = local_row_keys_cpu.size();
      key_batch_info.shared_buffer = new SharedBuffer();
      for (size_t j = 0; j < key_batch_info.rows.size(); j++) {
        RowKey row_key(key_batch_info.table, key_batch_info.rows[j]);
        local_row_keys_cpu.push_back(row_key);
      }
    }
  }

  /* Create GPU local storage */
  cout << "local_row_keys_gpu.size() = " << local_row_keys_gpu.size() << endl;
  DataStorage& local_storage_gpu = thread_data_ref.local_storage_gpu;
  local_storage_gpu.init(local_row_keys_gpu.size(), DataStorage::GPU);
  RowData *local_storage_ptr_gpu = local_storage_gpu.data();
  /* Create CPU local storage */
  cout << "local_row_keys_cpu.size() = " << local_row_keys_cpu.size() << endl;
  DataStorage& local_storage_cpu = thread_data_ref.local_storage_cpu;
  if (config.pinned_cpu_memory) {
    local_storage_cpu.init(local_row_keys_cpu.size(), DataStorage::PINNED_CPU);
  } else {
    local_storage_cpu.init(local_row_keys_cpu.size(), DataStorage::CPU);
  }
  RowData *local_storage_ptr_cpu = local_storage_cpu.data();

  /* Create index for accessing local storage */
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.local && opinfo.rows.size()) {
      TableRow first_key(opinfo.table, opinfo.rows[0]);
      CHECK(local_key_batches.count(first_key));
      LocalKeyBatchInfo& key_batch_info = local_key_batches[first_key];
      bool data_in_gpu_mem = key_batch_info.gpu;
      size_t first_key_index = key_batch_info.key_index;
      if (data_in_gpu_mem) {
        /* Data in GPU memory */
        opinfo.data_location = local_storage_gpu.type();
        CHECK_EQ(opinfo.data_location, DataStorage::GPU);
        opinfo.local_storage_ptr = &local_storage_ptr_gpu[first_key_index];
      } else {
        /* Data in CPU memory */
        opinfo.data_location = local_storage_cpu.type();
        CHECK_NE(opinfo.data_location, DataStorage::GPU);
        opinfo.shared_buffer = key_batch_info.shared_buffer;
        opinfo.local_storage_ptr = &local_storage_ptr_cpu[first_key_index];
      }
    }
  }

  /* Allocate CPU buffer */
  size_t size = max_nr_each_access * sizeof(RowData);
  mallocHost(&thread_data_ref.cpu_buffer, size);
  thread_data_ref.cpu_buffer_size = size;
  // cout << "max_nr_each_access = " << max_nr_each_access << endl;

  /* Make a thread cache, which is twice the peak size */
  size_t thread_cache_size;
  ngr_left = ngr_capacity - ngr_used;
  thread_cache_size = nr_being_used_peak * 2;
      /* Double the peak size */
  if (thread_cache_size > ngr_left) {
    CHECK_LT(config.mm_warning_level, 1);
    CHECK_EQ(thread_data_ref.ngr_used, 0);
    CHECK_EQ(ngr_capacity, ngr_left);
    cout << "*** WARNING: not enough space for double buffering\n";
    thread_cache_size = ngr_left;
  }
  CHECK_GE(thread_cache_size, nr_being_used_peak);
  thread_data_ref.thread_cache_size = thread_cache_size;
  thread_data_ref.ngr_used += thread_cache_size;
  CHECK_LE(thread_data_ref.ngr_used, ngr_capacity);
}

void ClientLib::vi_decide_param_cache() {
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;

  /* Decide the amount of memory used for each parameter cache entry */
  iter_t pf_slack = 0;
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (!opinfo.local && opinfo.type == OpInfo::READ) {
      if (opinfo.slack > pf_slack) {
        CHECK_EQ(pf_slack, 0);
        pf_slack = opinfo.slack;
      }
    }
  }
  size_t num_oplog_entries = 1;
  if (config.read_my_writes) {
    /* If we don't do read-my-writes, just one entry is enough */
    /* If we do read-my-writes, we need slack + 1 entries.
     * We always create the oplog entry of the current clock
     * at the first INC operation.
     * If the application only starts calling INC after finishing reading all
     * parameter data for the current clock, we need only (slack + 1)
     * oplog entries. Otherwise, we need (slack + 2). */
    num_oplog_entries = static_cast<size_t>(pf_slack) + 1;
  }
  /* Set info for all channels */
  for (size_t i = 0; i < comm_channels.size(); i++) {
    comm_channels[i].pf_slack = pf_slack;
    comm_channels[i].num_oplog_entries = num_oplog_entries;
  }
  size_t entries_per_row = 1 + num_oplog_entries;
      /* One process cache entry and num_oplog_entries oplog entries */

  /* Decide process cache data placement */
  RowKeys& row_keys_cpu = thread_data_ref.row_keys_cpu;
  RowKeys& row_keys_gpu = thread_data_ref.row_keys_gpu;
  boost::unordered_map<TableRow, bool> existing_keys;
  row_keys_gpu.clear();
  row_keys_cpu.clear();
  CHECK_LE(thread_data_ref.ngr_used, ngr_capacity);
  size_t ngr_param_cache_capacity = ngr_capacity - thread_data_ref.ngr_used;
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& opinfo = opseq[i];
    if (opinfo.type == OpInfo::CLOCK) {
      continue;
    }
    if (!opinfo.local && opinfo.rows.size()) {
      TableRow first_key(opinfo.table, opinfo.rows[0]);
      if (!existing_keys.count(first_key)) {
        /* New keys */
        if ((row_keys_gpu.size() + opinfo.rows.size()) * entries_per_row
              <= ngr_param_cache_capacity) {
          /* Insert to GPU key list */
          for (size_t j = 0; j < opinfo.rows.size(); j++) {
            TableRow table_row(opinfo.table, opinfo.rows[j]);
            CHECK(!existing_keys.count(table_row));
            RowKey row_key(opinfo.table, opinfo.rows[j]);
            row_keys_gpu.push_back(row_key);
            existing_keys[table_row] = true;
            opinfo.data_location = DataStorage::GPU;
          }
        } else {
          /* Insert to CPU key list */
          CHECK_LT(config.mm_warning_level, 3);
          for (size_t j = 0; j < opinfo.rows.size(); j++) {
            TableRow table_row(opinfo.table, opinfo.rows[j]);
            CHECK(!existing_keys.count(table_row));
            RowKey row_key(opinfo.table, opinfo.rows[j]);
            row_keys_cpu.push_back(row_key);
            existing_keys[table_row] = false;
            opinfo.data_location = DataStorage::CPU;
          }
        }
      } else {
        /* The keys exist */
        /* GPU or CPU */
        opinfo.data_location =
            existing_keys[first_key] ? DataStorage::GPU : DataStorage::CPU;
        for (size_t j = 0; j < opinfo.rows.size(); j++) {
          TableRow table_row(opinfo.table, opinfo.rows[j]);
          CHECK(existing_keys.count(table_row));
          CHECK_EQ(existing_keys[table_row],
              opinfo.data_location == DataStorage::GPU);
        }
      }
    }
  }

  /* Update memory usage */
  thread_data_ref.ngr_used += row_keys_gpu.size() * entries_per_row;
  CHECK_LE(thread_data_ref.ngr_used, ngr_capacity);

  /* Divide rows to channels.
   * Here we assume that all workers access the same set of rows,
   * so even though they decide row_id to channel_id mapping independently,
   * this decision is consistent across all workers. */
  cout << "row_keys_gpu.size() = " << row_keys_gpu.size() << endl;
  cout << "row_keys_cpu.size() = " << row_keys_cpu.size() << endl;
  rows_per_channel.resize(config.num_tables);
  vector<size_t> row_counts(config.num_tables);
  for (uint table_id = 0; table_id < config.num_tables; table_id++) {
    row_counts[table_id] = 0;
  }
  for (size_t i = 0; i < row_keys_gpu.size(); i++) {
    CHECK_LT(row_keys_gpu[i].table, row_counts.size());
    row_counts[row_keys_gpu[i].table]++;
  }
  for (size_t i = 0; i < row_keys_cpu.size(); i++) {
    CHECK_LT(row_keys_cpu[i].table, row_counts.size());
    row_counts[row_keys_cpu[i].table]++;
  }
  for (uint table_id = 0; table_id < config.num_tables; table_id++) {
    rows_per_channel[table_id] =
      (row_counts[table_id] + comm_channels.size() - 1)
          / comm_channels.size();
    // cout << "rows_per_channel of " << table_id
         // << " = " << rows_per_channel[table_id] << endl;
  }
}

void ClientLib::vi_create_thread_cache() {
  ThreadData& thread_data_ref = *thread_data;
  /* Use the rest available GPU memory as thread cache */
  CHECK_LE(thread_data_ref.ngr_used, ngr_capacity);
  thread_data_ref.thread_cache_size +=
      (ngr_capacity - thread_data_ref.ngr_used);
  cout << "thread_cache_size = " << thread_data_ref.thread_cache_size << endl;
  ThreadCache& thread_cache = thread_data_ref.thread_cache;
  thread_cache.init(thread_data_ref.thread_cache_size);
}

void ClientLib::vi_process_channel_table_finalize(
    ThreadData& thread_data_ref, uint channel_id, uint table_id, bool gpu) {
  RowKeys& access_row_keys =
      gpu ? thread_data_ref.row_keys_gpu :
            thread_data_ref.row_keys_cpu;
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = gpu ?
      cached_table.param_cache_gpu :
      cached_table.param_cache_cpu;
  ParamCacheIndex& param_cache_index = param_cache.param_cache_index;
  Oplog& oplog = param_cache.oplog;

  param_cache.num_rows = 0;
  for (size_t i = 0; i < access_row_keys.size(); i++) {
    table_id_t table = access_row_keys[i].table;
    row_idx_t row = access_row_keys[i].row;
    if (table != table_id) {
      /* Consider only the current table */
      continue;
    }
    if (get_channel_id(table, row) != channel_id) {
      /* Consider only the current channel */
      continue;
    }
    TableRow table_row(table, row);
    ParamCacheMetadata& param_cache_metadata =
        param_cache_index[table_row];
    /* NOTE: now I just assume cache_idx is the same as row_id */
    param_cache_metadata.cache_idx = param_cache.num_rows;
    param_cache_metadata.tablet_server_id = -2;
    param_cache.num_rows++;
  }

  /* Create param cache */
  if (gpu) {
    param_cache.data_cache.init(param_cache.num_rows,
        DataStorage::GPU);
    param_cache.data_cache.zerofy_data_gpu(thread_data_ref.cuda_stream);
  } else {
    param_cache.data_cache.init(param_cache.num_rows,
        DataStorage::CPU);
    param_cache.data_cache.zerofy_data_cpu();
  }
  CHECK(comm_channel.num_oplog_entries);
  param_cache.opmem_buffer_pool.init(
      gpu, comm_channel.num_oplog_entries, param_cache.num_rows);
  /* NOTE: this only works for the case where all clients access
   * the same set of rows. */
  param_cache.per_server_row_start.resize(num_servers);
  param_cache.per_server_num_rows.resize(num_servers);
  size_t div = param_cache.num_rows / num_servers;
  size_t res = param_cache.num_rows % num_servers;
  // cout << "param_cache.num_rows = " << param_cache.num_rows << endl;
  for (size_t i = 0; i < num_servers; i++) {
    param_cache.per_server_row_start[i] = div * i + (res > i ? i : res);
    param_cache.per_server_num_rows[i] = div + (res > i ? 1 : 0);
  }
  /* TODO: we don't actually need so many oplog entries.
   * It can be a circular array */
  oplog.resize(MAX_CLOCK);
  for (ParamCacheIndex::iterator table_row_it
         = param_cache_index.begin();
       table_row_it != param_cache_index.end(); table_row_it++) {
    const TableRow& table_row = table_row_it->first;
    table_id_t table = table_row.first;
    row_idx_t row = table_row.second;
    ParamCacheMetadata& param_cache_metadata =
        param_cache_index[table_row];
    size_t row_idx = param_cache_metadata.cache_idx;
    param_cache_metadata.row_idx = row_idx;
    param_cache.data_cache.row_keys[row_idx].table = table;
    param_cache.data_cache.row_keys[row_idx].row = row;
  }
}

void ClientLib::vi_process_channel_finalize(
    ThreadData& thread_data_ref, uint channel_id, bool gpu) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  for (uint table_id = 0; table_id < config.num_tables; table_id++) {
    vi_process_channel_table_finalize(
        thread_data_ref, channel_id, table_id, gpu);
  }

  /* Create send/receive buffers (only necessary for the GPU cache) */
  if (gpu) {
    /* Decide the size of the biggest table */
    size_t biggest_table_size = 0;
    for (uint table_id = 0; table_id < config.num_tables; table_id++) {
      size_t table_size =
          comm_channel.cached_tables[table_id].param_cache_gpu.num_rows;
      biggest_table_size =
          table_size > biggest_table_size ? table_size : biggest_table_size;
    }
    size_t size = biggest_table_size * sizeof(RowData);
    comm_channel.comm_buffer_size = size;
    mallocHost(&comm_channel.send_buffer, size);
    mallocHost(&comm_channel.recv_buffer, size);
  }
}

void ClientLib::vi_process_finalize() {
  ThreadData& thread_data_ref = *thread_data;
  size_t thread_id = thread_data_ref.thread_id;
  if (nr_threads >= num_channels) {
    size_t threads_per_channel = nr_threads / num_channels;
    CHECK(threads_per_channel);
    bool is_leading = (thread_id % threads_per_channel == 0);
    if (!is_leading) {
      return;
    }
    uint channel_id = thread_id / threads_per_channel;
    if (channel_id >= num_channels) {
      return;
    }
    vi_process_channel_finalize(
        thread_data_ref, channel_id, false /* cpu */);
    vi_process_channel_finalize(
        thread_data_ref, channel_id, true /* gpu */);
  } else {
    size_t div = num_channels / nr_threads;
    size_t res = num_channels % nr_threads;
    size_t start = div * thread_id + (res > thread_id ? thread_id : res);
    size_t size = div + (res > thread_id ? 1 : 0);
    for (uint channel_id = start; channel_id < start + size; channel_id++) {
      vi_process_channel_finalize(
          thread_data_ref, channel_id, false /* cpu */);
      vi_process_channel_finalize(
          thread_data_ref, channel_id, true /* gpu */);
    }
  }
}

void ClientLib::vi_thread_finalize() {
  ThreadData& thread_data_ref = *thread_data;
  OpSeq& opseq = thread_data_ref.opseq;
  
  /* Decide last write of each table */
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& op_info = opseq[i];
    if (op_info.type == OpInfo::WRITE || op_info.type == OpInfo::POST_READ) {
      uint prestep_op_id = static_cast<uint>(op_info.prestep_handle);
      CHECK_LT(prestep_op_id, opseq.size());
      OpInfo& prestep_op_info = opseq[prestep_op_id];
      op_info.local = prestep_op_info.local;
      op_info.table_id = prestep_op_info.table_id;
    }
  }
  vector<bool> table_last_writes(config.num_tables);
  for (uint table_id = 0; table_id < table_last_writes.size(); table_id++) {
    table_last_writes[table_id] = false;
  }
  bool clock_op_seen = false;
  for (size_t i = opseq.size() - 1; i >= 0 && i < opseq.size(); i--) {
    OpInfo& op_info = opseq[i];
    if (op_info.type == OpInfo::CLOCK) {
      clock_op_seen = true;
    }
    if (!clock_op_seen) {
      /* Operations after CLOCK are all unrepeated ones */
      continue;
    }
    if (!op_info.local && op_info.type == OpInfo::WRITE) {
      uint table_id = op_info.table_id;
      CHECK_LT(table_id, table_last_writes.size());
      if (!table_last_writes[table_id]) {
        table_last_writes[table_id] = true;
        op_info.table_last_write = true;
      } else {
        op_info.table_last_write = false;
      }
    }
  }
  for (uint table_id = 0; table_id < table_last_writes.size(); table_id++) {
    CHECK(table_last_writes[table_id])
        << "No one writes table " << table_id;
  }

  /* Create index for accessing process cache */
  for (size_t i = 0; i < opseq.size(); i++) {
    OpInfo& op_info = opseq[i];
    if (!op_info.local &&
        (op_info.type == OpInfo::READ || op_info.type == OpInfo::PRE_WRITE)) {
      /* Double index: row_in_cache[index0[i]] -> accessed_row[index1[i]]
       * It should be used when the number of rows accessed
       * in this batch is much smaller than the total number of rows
       * (e.g., less than half). */
      vi_create_double_index(op_info);
    }
  }
}

void ClientLib::vi_create_double_index(OpInfo& opinfo) {
  ThreadData& thread_data_ref = *thread_data;
  opinfo.index_type = OpInfo::DOUBLE_INDEX;
  vector<vector<DoubleIndex> > row_index_tmp;
  row_index_tmp.resize(num_channels);
  for (size_t j = 0; j < opinfo.rows.size(); j++) {
    TableRow table_row(opinfo.table, opinfo.rows[j]);
    uint channel_id = get_channel_id(table_row);
    CHECK_LT(channel_id, comm_channels.size());
    CommunicationChannel& comm_channel = comm_channels[channel_id];
    uint table_id = opinfo.table_id;
    CHECK_LT(table_id, comm_channel.cached_tables.size());
    CachedTable& cached_table = comm_channel.cached_tables[table_id];
    ParamCache& param_cache = opinfo.data_location == DataStorage::GPU ?
        cached_table.param_cache_gpu :
        cached_table.param_cache_cpu;
    ParamCacheIndex& param_cache_index = param_cache.param_cache_index;
    ParamCacheIndex::iterator index_it = param_cache_index.find(table_row);
    CHECK(index_it != param_cache_index.end())
        << "row = " << opinfo.rows[j]
        << ", data_location = " << opinfo.data_location;
    ParamCacheMetadata& param_cache_metadata = index_it->second;
    size_t cache_idx = param_cache_metadata.cache_idx;
    CHECK_LT(cache_idx, param_cache.num_rows);
    row_index_tmp[channel_id].push_back(DoubleIndex(j, cache_idx));
  }
  /* Allocate and create index in CPU memory */
  opinfo.row_index_size = opinfo.rows.size();
  size_t index_memsize = opinfo.rows.size() * sizeof(DoubleIndex);
  opinfo.row_index_cpu = reinterpret_cast<DoubleIndex *>(malloc(index_memsize));
  CHECK(opinfo.row_index_cpu);
  opinfo.index_infos.resize(num_channels);
  size_t current_index = 0;
  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {
    PerChannelIndexInfo& index_info = opinfo.index_infos[channel_id];
    index_info.index_start = current_index;
    index_info.index_size = row_index_tmp[channel_id].size();
    size_t index_range = 0;
    if (row_index_tmp[channel_id].size()) {
      size_t index_min = row_index_tmp[channel_id][0].id1;
      size_t index_max = row_index_tmp[channel_id][0].id1;
      for (size_t index_id = 0;
           index_id < row_index_tmp[channel_id].size(); index_id++) {
        opinfo.row_index_cpu[current_index++] =
            row_index_tmp[channel_id][index_id];
        if (row_index_tmp[channel_id][index_id].id1 < index_min) {
          index_min = row_index_tmp[channel_id][index_id].id1;
        }
        if (row_index_tmp[channel_id][index_id].id1 > index_max) {
          index_max = row_index_tmp[channel_id][index_id].id1;
        }
      }
      index_range = index_max - index_min + 1;
    }
    /* We think the indexed rows should be contiguous.
     * So maybe memcpy can be used instead? */
    CHECK_EQ(index_range, row_index_tmp[channel_id].size());
  }
  CHECK_EQ(current_index, opinfo.rows.size());
  if (opinfo.data_location == DataStorage::GPU) {
    /* Allocate GPU index memory and copy CPU index to GPU index */
    CUDA_CHECK(cudaMalloc(&opinfo.row_index_gpu, index_memsize));
    CUDA_CHECK(cudaMemcpyAsync(opinfo.row_index_gpu, opinfo.row_index_cpu,
        index_memsize, cudaMemcpyDefault, thread_data_ref.cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(thread_data_ref.cuda_stream));
  }
}
