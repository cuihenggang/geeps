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

/* GeePS tablet server */

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#include <utility>
#include <string>
#include <vector>

#include "common/internal-config.hpp"
#include "server-encoder-decoder.hpp"
#include "tablet-server.hpp"

using std::string;
using std::cerr;
using std::cout;
using std::endl;
using std::vector;
using std::pair;
using std::make_pair;
using boost::format;
using boost::lexical_cast;
using boost::shared_ptr;
using boost::make_shared;

TabletStorage::TabletStorage(
    uint channel_id, uint num_channels, uint process_id, uint num_processes,
    shared_ptr<ServerClientEncode> communicator,
    cudaStream_t cuda_stream, cublasHandle_t cublas_handle,
    const GeePsConfig& config) :
      channel_id(channel_id), num_channels(num_channels),
      process_id(process_id), num_processes(num_processes),
      num_clients(num_processes),
      communicator(communicator),
      cuda_stream(cuda_stream), cublas_handle(cublas_handle),
      config(config) {
  /* Initialize data tables */
  data_tables.resize(config.num_tables);
  for (uint table_id = 0; table_id < data_tables.size(); table_id++) {
    DataTable& data_table = data_tables[table_id];
    data_table.vec_clock.resize(num_processes);
    for (uint client_id = 0; client_id < num_processes; client_id++) {
      data_table.vec_clock[client_id] = INITIAL_DATA_AGE;
    }
    data_table.global_clock = INITIAL_DATA_AGE;
    data_table.row_count = 0;
  }
}

void TabletStorage::update_row_batch(
      uint client_id, iter_t clock, uint table_id,
      RowKey *row_keys, RowOpVal *updates, uint batch_size) {
  server_stats.nr_update += batch_size;
  if (client_id == process_id) {
    server_stats.nr_local_update += batch_size;
  }

  CHECK_LT(table_id, data_tables.size());
  DataTable& data_table = data_tables[table_id];
  DataStorage& data_store = data_table.store;
  
  CHECK_LT(client_id, data_table.vec_clock.size());
  iter_t cur_clock = data_table.vec_clock[client_id];
  if (cur_clock != INITIAL_DATA_AGE && clock != cur_clock + 1) {
    cerr << "WARNING CS clocks out of sync,"
         << " client = " << client_id
         << " clock = " << clock
         << " cur_clock = " << cur_clock
         << endl;
    CHECK(0);
  }

  if (batch_size == 0) {
    return;
  }

  if (data_store.size() == 0) {
    data_store.init(batch_size, DataStorage::CPU);
    data_store.zerofy_data_cpu();
    data_table.row_count = batch_size;
    memcpy(data_store.row_keys.data(), row_keys,
        batch_size * sizeof(RowKey));
  }
  CHECK_EQ(data_store.size(), batch_size);
  apply_updates(table_id, updates, batch_size);
}

void TabletStorage::apply_updates(
    uint table_id, RowOpVal *update_rows, size_t batch_size) {
  CHECK_LT(table_id, data_tables.size());
  DataTable& data_table = data_tables[table_id];
  DataStorage& data_store = data_table.store;
  
  val_t *update = reinterpret_cast<val_t *>(update_rows);
  CHECK_EQ(data_store.size(), batch_size);
  val_t *master_data = reinterpret_cast<val_t *>(data_store.data());
  size_t num_vals = batch_size * ROW_DATA_SIZE;

  cpu_add(num_vals,
      master_data,
      update,
      master_data);
}

void TabletStorage::process_multiclient_pending_reads(
    iter_t clock, uint table_id) {
  /* Rotate the starting client */
  uint client_to_start = clock % num_clients;
  /* Process pending reads */
  for (uint i = 0; i < num_clients; i++) {
    uint client_id = (client_to_start + i) % num_clients;
    process_pending_reads(client_id, clock, table_id);
  }
}

void TabletStorage::process_pending_reads(
    uint client_id, iter_t clock, uint table_id) {
  /* NOTE: we assume each client uses all the rows */
  CHECK_LT(table_id, data_tables.size());
  DataTable& data_table = data_tables[table_id];
  DataStorage& data_store = data_table.store;

  RowKeys& row_keys = data_store.row_keys;
  CHECK_EQ(data_store.size(), row_keys.size());
  RowData *row_data = data_store.data();
  CHECK_EQ(data_table.global_clock, clock);
  iter_t data_age = data_table.global_clock;
  iter_t self_clock = data_table.vec_clock[client_id];
  communicator->read_row_batch_reply(
      client_id, process_id, data_age, self_clock, table_id,
      row_keys.data(), row_data, row_keys.size());
}

void TabletStorage::reset_perf_counters() {
  server_stats.reset();
}

void TabletStorage::clock(uint client_id, iter_t clock, uint table_id) {
  int timing = true;
  tbb::tick_count clock_ad_start;
  tbb::tick_count clock_ad_apply_op_end;
  tbb::tick_count clock_ad_end;

  if (timing) {
    clock_ad_start = tbb::tick_count::now();
  }

  CHECK_LT(table_id, data_tables.size());
  DataTable& data_table = data_tables[table_id];

  CHECK_LT(client_id, data_table.vec_clock.size());
  if (data_table.vec_clock[client_id] != INITIAL_DATA_AGE) {
    CHECK_EQ(clock, data_table.vec_clock[client_id] + 1);
  }
  data_table.vec_clock[client_id] = clock;

  iter_t new_global_clock = clock_min(data_table.vec_clock);
  if (new_global_clock != data_table.global_clock) {
    if (data_table.global_clock != INITIAL_DATA_AGE) {
      CHECK_EQ(new_global_clock, data_table.global_clock + 1);
    }
    data_table.global_clock = new_global_clock;

    /* Send pending read requests */
    process_multiclient_pending_reads(
        data_table.global_clock, table_id);

    /* Notify clients of new iteration */
    /* We don't need to do that now, because the client will use
     * the reception of row data as the notification. */
    // communicator->clock(process_id, global_clock);
  }

  if (timing) {
    clock_ad_end = tbb::tick_count::now();
    server_stats.clock_ad_send_pending_time +=
      (clock_ad_end - clock_ad_apply_op_end).seconds();
    server_stats.clock_ad_time_tot +=
      (clock_ad_end - clock_ad_start).seconds();
  }
}

void TabletStorage::get_stats(
      uint client_id, shared_ptr<MetadataServer> metadata_server) {
  server_stats.nr_rows = 0;
  for (uint table_id = 0; table_id < data_tables.size(); table_id++) {
    server_stats.nr_rows += data_tables[table_id].row_count;
  }

  std::stringstream combined_server_stats;
  combined_server_stats << "{"
         << "\"storage\": " << server_stats.to_json() << ", "
         << "\"metadata\": " << metadata_server->get_stats() << ", "
         << "\"router\": " << communicator->get_router_stats()
         << " } ";
  communicator->get_stats(client_id, combined_server_stats.str());
}
