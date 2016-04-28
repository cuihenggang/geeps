/*
 * Copyright (C) 2013 by Carnegie Mellon University.
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
