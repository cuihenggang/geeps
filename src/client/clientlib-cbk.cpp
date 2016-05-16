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

void ClientLib::find_row_cbk(
    table_id_t table, row_idx_t row, uint32_t server_id) {
  CHECK(0);
}

void ClientLib::recv_row_batch_cbk(
      uint channel_id, iter_t data_age, iter_t self_clock, uint server_id,
      uint table_id, RowKey *row_keys, RowData *row_data, uint batch_size) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  BgthreadStats& bgthread_stats = comm_channel.bgthread_stats;
  bool timing = true;
  tbb::tick_count recv_row_start;
  tbb::tick_count apply_op_start;
  tbb::tick_count recv_row_end;
  if (timing) {
    recv_row_start = tbb::tick_count::now();
  }

  recv_row_batch(
      channel_id, server_id, table_id,
      row_keys, row_data, batch_size,
      data_age, self_clock, true);

  if (timing) {
    recv_row_end = tbb::tick_count::now();
    bgthread_stats.tot_recv_row_time +=
      (recv_row_end - recv_row_start).seconds();
  }
}

void ClientLib::server_clock_cbk(
        uint channel_id, uint server_id, iter_t clock, uint table_id) {
  CHECK_GE(clock, 0);
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  CHECK_LT(server_id, cached_table.server_clock.size());
  CHECK_LE(cached_table.server_clock[server_id], clock);
  cached_table.server_clock[server_id] = clock;

  iter_t min_clock = clock_min(cached_table.server_clock);
  if (min_clock > cached_table.server_clock_min) {
    /* Remove oplog entries.
     * We don't grab any locks because we believe there won't be any threads
     * accessing it. */
    iter_t start_clock = cached_table.server_clock_min + 1;
    iter_t end_clock = min_clock;
    reclaim_oplog(
        channel_id, start_clock, end_clock, table_id, true /* gpu */);
    reclaim_oplog(
        channel_id, start_clock, end_clock, table_id, false /* cpu */);
    cached_table.server_clock_min = min_clock;
  }
}

void ClientLib::get_stats_cbk(const string& server_stats) {
  unique_lock<mutex> lock(global_mutex);
  proc_stats.server_stats = server_stats;
  proc_stats.server_stats_refreshed = true;
  global_cvar.notify_all();
}
