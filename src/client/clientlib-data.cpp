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

bool ClientLib::recv_row_batch(
      uint channel_id, uint server_id, uint table_id,
      RowKey *row_keys, RowData *row_data, uint batch_size,
      iter_t data_age, iter_t self_clock, bool timing) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];

  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  uint cache_size_cpu =
      cached_table.param_cache_cpu.per_server_num_rows[server_id];
  uint cache_size_gpu =
      cached_table.param_cache_gpu.per_server_num_rows[server_id];
  CHECK_EQ(cache_size_cpu + cache_size_gpu, batch_size);

  /* Copy to pinned memory first */
  /* GPU data is stored after CPU data */
  RowData *row_data_gpu_to_be_copied = &row_data[cache_size_cpu];
  size_t size = cache_size_gpu * sizeof(RowData);
  if (size) {
    /* TODO: check is the recv_buffer size big enough */
    memcpy(comm_channel.recv_buffer, row_data_gpu_to_be_copied, size);
  }

  unique_lock<mutex> channel_lock(*comm_channel.mutex);

  CHECK_LT(server_id, cached_table.per_server_data_age.size());
  iter_t& server_data_age = cached_table.per_server_data_age[server_id];

  if (data_age < server_data_age) {
    cerr << "WARNING: old data received! "
         << data_age << " VS " << server_data_age
         << endl;
    CHECK(0);
    return true;
  }
  if (data_age == server_data_age) {
    /* It's a duplicate that we have already received */
    CHECK(0);
    return true;
  }
  CHECK_LE(data_age, self_clock);
  server_data_age = data_age;
  cached_table.data_age = clock_min(cached_table.per_server_data_age);

  RowKey *row_keys_cpu = row_keys;
  RowData *row_data_cpu = row_data;
  RowKey *row_keys_gpu = &row_keys[cache_size_cpu];
  RowData *row_data_gpu = comm_channel.recv_buffer;
  recv_row_batch_cpu(channel_id, server_id, row_keys_cpu, row_data_cpu,
      cache_size_cpu, data_age, self_clock, table_id);
  recv_row_batch_gpu(channel_id, server_id, row_keys_gpu, row_data_gpu,
      cache_size_gpu, data_age, self_clock, table_id);

  /* TODO: move mutex and cvar to table level */
  comm_channel.cvar->notify_all();

  return true;
}

void ClientLib::recv_row_batch_gpu(
      uint channel_id, uint server_id,
      RowKey *row_keys, RowData *row_data, uint batch_size,
      iter_t data_age, iter_t self_clock, uint table_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_gpu;
  DataCache& data_cache = param_cache.data_cache;
  CHECK_LT(server_id, param_cache.per_server_row_start.size());
  CHECK_LT(server_id, param_cache.per_server_num_rows.size());
  uint row_start = param_cache.per_server_row_start[server_id];
  uint num_rows = param_cache.per_server_num_rows[server_id];
  CHECK_EQ(num_rows, batch_size);
  RowData *cached_data = &data_cache.data()[row_start];
  cudaStream_t& cuda_stream = comm_channel.cuda_stream_recv;
  cublasHandle_t& cublas_handle = comm_channel.cublas_handle_recv;

  CUDA_CHECK(cudaMemcpyAsync(cached_data, row_data,
      batch_size * sizeof(RowData), cudaMemcpyDefault, cuda_stream));
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));

  if (config.read_my_writes) {
    /* Apply oplogs */
    Oplog& oplog = param_cache.oplog;
    int64_t oplog_count = 0;
    CHECK_LE(data_age, self_clock);
    for (iter_t clock = self_clock + 1; clock <= fast_clock; clock++) {
      if (oplog[clock] == NULL) {
        continue;
      }
      FlatOps& flat_ops = *oplog[clock];
      if (flat_ops.flag == FlatOps::INC) {
        CHECK_EQ(flat_ops.size(), data_cache.size());
        oplog_count++;
        const RowOpVal *updates = &flat_ops.data()[row_start];
        add_row_batch_gpu(cublas_handle, cached_data, updates, num_rows);
      }
    }
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  }
}

void ClientLib::recv_row_batch_cpu(
      uint channel_id, uint server_id,
      RowKey *row_keys, RowData *row_data, uint batch_size,
      iter_t data_age, iter_t self_clock, uint table_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_cpu;
  DataCache& data_cache = param_cache.data_cache;
  CHECK_LT(server_id, param_cache.per_server_row_start.size());
  CHECK_LT(server_id, param_cache.per_server_num_rows.size());
  uint row_start = param_cache.per_server_row_start[server_id];
  uint num_rows = param_cache.per_server_num_rows[server_id];
  CHECK_EQ(num_rows, batch_size);
  RowData *cached_data = &data_cache.data()[row_start];

  memcpy(cached_data, row_data, batch_size * sizeof(RowData));

  if (config.read_my_writes) {
    /* Apply oplogs */
    Oplog& oplog = param_cache.oplog;
    int64_t oplog_count = 0;
    CHECK_LE(data_age, self_clock);
    for (iter_t clock = self_clock + 1; clock <= fast_clock; clock++) {
      if (oplog[clock] == NULL) {
        continue;
      }
      FlatOps& flat_ops = *oplog[clock];
      if (flat_ops.flag == FlatOps::INC) {
        CHECK_EQ(flat_ops.size(), data_cache.size());
        oplog_count++;
        const RowOpVal *updates = &flat_ops.data()[row_start];
        add_row_batch(cached_data, updates, num_rows);
      }
    }
  }
}

int ClientLib::read_row_batch_param_cache(
    RowData *buffer, OpInfo& op_info, iter_t clock,
    cudaStream_t& cuda_stream, RowData *cpu_buffer,
    bool stat, bool timing) {
  CHECK_EQ(op_info.type, OpInfo::READ);
  uint table_id = op_info.table_id;

  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {
    CommunicationChannel& comm_channel = comm_channels[channel_id];
    unique_lock<mutex> channel_lock(*comm_channel.mutex);

    CHECK_LT(table_id, comm_channel.cached_tables.size());
    CachedTable& cached_table = comm_channel.cached_tables[table_id];
  
    while (cached_table.data_age < clock) {
      if (!comm_channel.cvar->timed_wait(channel_lock,
            boost::posix_time::milliseconds(12000))) {
        if (channel_id == 0) {
          /* Read timeout */
          cerr << "machine " << process_id
                << " wait time out!" << endl;
          cerr << " Need: " << clock
               << " Data age: " << cached_table.data_age
               << std::endl;
           /* Read time out */
        }
      }
    }

    if (buffer == NULL) {
      return 1;
    }

    tbb::tick_count time_start = tbb::tick_count::now();
    if (op_info.data_location == DataStorage::GPU) {
      read_batch_gpu(buffer, op_info, channel_id, cuda_stream);
    } else {
      CHECK(cpu_buffer);
      read_batch_cpu(cpu_buffer, op_info, channel_id);
    }
    proc_stats.bg_read_read_time +=
        (tbb::tick_count::now() - time_start).seconds();
  }

  tbb::tick_count time_start = tbb::tick_count::now();
  if (op_info.data_location != DataStorage::GPU) {
    /* Copy the data from CPU buffer to GPU buffer */
    size_t size;
    if (op_info.num_vals_limit >= 0) {
      size = op_info.num_vals_limit * sizeof(val_t);
    } else {
      size = op_info.rows.size() * sizeof(RowData);
    }
    CUDA_CHECK(cudaMemcpyAsync(buffer, cpu_buffer, size,
        cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  }
  proc_stats.bg_read_move_time +=
      (tbb::tick_count::now() - time_start).seconds();

  return 1;
}

void ClientLib::read_batch_gpu(
    RowData *buffer, OpInfo& op_info, uint channel_id,
    cudaStream_t& cuda_stream) {
  /* Channel lock is held while calling this function */
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  uint table_id = op_info.table_id;
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_gpu;
  DataCache& data_cache = param_cache.data_cache;
  CHECK(op_info.row_index_gpu);
  const DoubleIndex *row_index = op_info.row_index_gpu;
  PerChannelIndexInfo& index_info = op_info.index_infos[channel_id];
  size_t row_index_start = index_info.index_start;
  size_t row_index_size = index_info.index_size;
  size_t num_rows = param_cache.num_rows;
  CHECK_EQ(num_rows, data_cache.size());
  const DoubleIndex *row_channel_index = &row_index[row_index_start];
  DoubleIndex index_offset(0, 0);
      /* No offset needed */
  assign_rows_to_double_index_gpu(
      buffer, data_cache.data(), row_channel_index, row_index_size,
      index_offset, ROW_DATA_SIZE, op_info.num_vals_limit, cuda_stream);
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
}

void ClientLib::read_batch_cpu(
    RowData *buffer, OpInfo& op_info, uint channel_id) {
  /* Channel lock is held while calling this function */
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  uint table_id = op_info.table_id;
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_cpu;
  DataCache& data_cache = param_cache.data_cache;
  CHECK(op_info.row_index_cpu);
  const DoubleIndex *row_index = op_info.row_index_cpu;
  PerChannelIndexInfo& index_info = op_info.index_infos[channel_id];
  size_t row_index_start = index_info.index_start;
  size_t row_index_size = index_info.index_size;
  size_t num_rows = param_cache.num_rows;
  CHECK_EQ(num_rows, data_cache.size());
  const DoubleIndex *row_channel_index = &row_index[row_index_start];
  DoubleIndex index_offset(0, 0);
      /* No offset needed */
  assign_rows_to_double_index_cpu(
      buffer, data_cache.data(), row_channel_index, row_index_size,
      index_offset, ROW_DATA_SIZE, op_info.num_vals_limit);
}

bool ClientLib::update_batch_param_cache(
    OpInfo& op_info, const RowOpVal *updates, iter_t clock,
    cudaStream_t& cuda_stream, RowData *cpu_buffer, bool stat, bool timing) {

  CHECK_EQ(op_info.type, OpInfo::PRE_WRITE);

  if (op_info.data_location != DataStorage::GPU) {
    /* Copy the updates from GPU buffer to CPU buffer */
    tbb::tick_count time_start = tbb::tick_count::now();
    CHECK(cpu_buffer);
    size_t size;
    if (op_info.num_vals_limit >= 0) {
      size = op_info.num_vals_limit * sizeof(val_t);
    } else {
      size = op_info.rows.size() * sizeof(RowData);
    }
    CUDA_CHECK(cudaMemcpyAsync(cpu_buffer, updates, size,
        cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
    proc_stats.bg_write_move_time +=
        (tbb::tick_count::now() - time_start).seconds();
  }

  tbb::tick_count time_start = tbb::tick_count::now();
  for (uint channel_id = 0; channel_id < num_channels; channel_id++) {

    CommunicationChannel& comm_channel = comm_channels[channel_id];
    unique_lock<mutex> channel_lock(*comm_channel.mutex);

    if (op_info.data_location == DataStorage::GPU) {
      update_batch_gpu(op_info, updates, channel_id, clock, cuda_stream);
    } else {
      update_batch_cpu(op_info, cpu_buffer, channel_id, clock);
    }

  }
  proc_stats.bg_write_update_time +=
      (tbb::tick_count::now() - time_start).seconds();

  return true;
}

void ClientLib::update_batch_gpu(
    OpInfo& op_info, const RowOpVal *updates, uint channel_id, iter_t clock,
    cudaStream_t& cuda_stream) {
  /* Channel lock is held while calling this function */
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  uint table_id = op_info.table_id;
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_gpu;
  DataCache& data_cache = param_cache.data_cache;
  if (param_cache.oplog[clock] == NULL) {
    create_oplog_entry(channel_id, clock, table_id, true);
    FlatOps *new_flatops = param_cache.oplog[clock];
    CHECK(new_flatops);
    /* The DataStorage class zerofies data using cudaMemsetAsync(),
     * which takes in a cudaStream_t argument.
     * a cudaStreamSynchronize() will be called on this stream.
     * There was a bug about this that I spent almost two weeks debugging.
     * Previously, I was using cudaMemset(),
     * and I saw "unspecified launch failure" in weird places.
     * Finally I realized that cudaMemset() is actually *asynchronous*,
     * in that it returns control to host code before finish.
     * So we actually need a cudaDeviceSynchronize() after it.
     * But any way, we should use cudaMemsetAsync() here. */
    new_flatops->zerofy_data_gpu(cuda_stream);
  }
  FlatOps& flat_ops = *param_cache.oplog[clock];
  CHECK(op_info.row_index_gpu);
  const DoubleIndex *row_index = op_info.row_index_gpu;
  PerChannelIndexInfo& index_info = op_info.index_infos[channel_id];
  size_t row_index_start = index_info.index_start;
  size_t row_index_size = index_info.index_size;
  size_t num_rows = param_cache.num_rows;
  CHECK_EQ(num_rows, data_cache.size());
  CHECK_EQ(num_rows, flat_ops.size());
  flat_ops.flag = FlatOps::INC;
  const DoubleIndex *row_channel_index = &row_index[row_index_start];
  DoubleIndex index_offset(0, 0);
      /* No offset needed */
  add_rows_from_double_index_gpu(
      flat_ops.data(), updates, row_channel_index, row_index_size,
      index_offset, ROW_DATA_SIZE, op_info.num_vals_limit,
      cuda_stream);
  if (config.read_my_writes) {
    add_rows_from_double_index_gpu(
        data_cache.data(), updates, row_channel_index, row_index_size,
        index_offset, ROW_DATA_SIZE, op_info.num_vals_limit,
        cuda_stream);
  }
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
}

void ClientLib::update_batch_cpu(
    OpInfo& op_info, const RowOpVal *updates, uint channel_id, iter_t clock) {
  /* Channel lock is held while calling this function */
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  uint table_id = op_info.table_id;
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = cached_table.param_cache_cpu;
  DataCache& data_cache = param_cache.data_cache;
  if (param_cache.oplog[clock] == NULL) {
    create_oplog_entry(channel_id, clock, table_id, false);
    FlatOps *new_flatops = param_cache.oplog[clock];
    CHECK(new_flatops);
    new_flatops->zerofy_data_cpu();
  }
  FlatOps& flat_ops = *param_cache.oplog[clock];
  CHECK(op_info.row_index_cpu);
  const DoubleIndex *row_index = op_info.row_index_cpu;
  PerChannelIndexInfo& index_info = op_info.index_infos[channel_id];
  size_t row_index_start = index_info.index_start;
  size_t row_index_size = index_info.index_size;
  size_t num_rows = param_cache.num_rows;
  CHECK_EQ(num_rows, data_cache.size());
  CHECK_EQ(num_rows, flat_ops.size());
  flat_ops.flag = FlatOps::INC;
  const DoubleIndex *row_channel_index = &row_index[row_index_start];
  DoubleIndex index_offset(0, 0);
      /* No offset needed */
  add_rows_from_double_index_cpu(
      flat_ops.data(), updates, row_channel_index, row_index_size,
      index_offset, ROW_DATA_SIZE, op_info.num_vals_limit);
  if (config.read_my_writes) {
    add_rows_from_double_index_cpu(
        data_cache.data(), updates, row_channel_index, row_index_size,
        index_offset, ROW_DATA_SIZE, op_info.num_vals_limit);
  }
}

void ClientLib::push_updates_param_cache(
      uint channel_id, iter_t clock, uint table_id) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  BgthreadStats& bgthread_stats = comm_channel.bgthread_stats;
  tbb::tick_count push_updates_start = tbb::tick_count::now();

  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];

  ParamCache& param_cache_cpu = cached_table.param_cache_cpu;
  Oplog& oplog_cpu = param_cache_cpu.oplog;
  ParamCache& param_cache_gpu = cached_table.param_cache_gpu;
  Oplog& oplog_gpu = param_cache_gpu.oplog;
  bool cpu_op = oplog_cpu.size() != 0 && oplog_cpu[clock] != NULL;
  bool gpu_op = oplog_gpu.size() != 0 && oplog_gpu[clock] != NULL;
  if (!cpu_op && !gpu_op) {
    comm_channel.encoder->clock_broadcast(clock, table_id);
    return;
  }

  const RowKey *row_keys_cpu = NULL;
  const RowOpVal *updates_cpu = NULL;
  if (cpu_op) {
    FlatOps& flat_ops_cpu = *oplog_cpu[clock];
    row_keys_cpu = flat_ops_cpu.row_keys.data();
    updates_cpu = flat_ops_cpu.data();
    CHECK_EQ(param_cache_cpu.per_server_row_start.size(), num_servers);
    CHECK_EQ(param_cache_cpu.per_server_num_rows.size(), num_servers);
  }

  const RowKey *row_keys_gpu = NULL;
  RowOpVal *updates_gpu = NULL;
  if (gpu_op) {
    /* Copy GPU updates to host memory buffer */
    FlatOps& flat_ops_gpu = *oplog_gpu[clock];
    row_keys_gpu = flat_ops_gpu.row_keys.data();
    updates_gpu = comm_channel.send_buffer;
    CUDA_CHECK(cudaMemcpyAsync(updates_gpu, flat_ops_gpu.data(),
        flat_ops_gpu.memsize(), cudaMemcpyDefault,
        comm_channel.cuda_stream_send));
    CUDA_CHECK(cudaStreamSynchronize(comm_channel.cuda_stream_send));
    CHECK_EQ(param_cache_gpu.per_server_row_start.size(), num_servers);
    CHECK_EQ(param_cache_gpu.per_server_num_rows.size(), num_servers);
    if (!config.read_my_writes) {
      bool gpu = true;
      reclaim_oplog(
        channel_id, clock, clock, table_id, gpu);
    }
  }

  /* Send to each server */
  for (uint server_id = 0; server_id < num_servers; server_id++) {
    const RowOpVal *updates_to_send_cpu = NULL;
    const RowKey *row_keys_to_send_cpu = NULL;
    uint num_rows_cpu = 0;
    const RowOpVal *updates_to_send_gpu = NULL;
    const RowKey *row_keys_to_send_gpu = NULL;
    uint num_rows_gpu = 0;
    if (cpu_op) {
      uint row_start_cpu = param_cache_cpu.per_server_row_start[server_id];
      updates_to_send_cpu = &updates_cpu[row_start_cpu];
      row_keys_to_send_cpu = &row_keys_cpu[row_start_cpu];
      num_rows_cpu = param_cache_cpu.per_server_num_rows[server_id];
    }
    if (gpu_op) {
      uint row_start_gpu = param_cache_gpu.per_server_row_start[server_id];
      updates_to_send_gpu = &updates_gpu[row_start_gpu];
      row_keys_to_send_gpu = &row_keys_gpu[row_start_gpu];
      num_rows_gpu = param_cache_gpu.per_server_num_rows[server_id];
    }
    comm_channel.encoder->clock_with_updates_batch(server_id, clock, table_id,
        updates_to_send_cpu, row_keys_to_send_cpu, num_rows_cpu,
        updates_to_send_gpu, row_keys_to_send_gpu, num_rows_gpu);
  }

  if (cpu_op) {
    if (!config.read_my_writes) {
      /* If we don't do read-my-writes, we can reclaim the oplogs here */
      bool gpu = false;
      reclaim_oplog(
          channel_id, clock, clock, table_id, gpu);
    }
  }

  bgthread_stats.tot_push_updates_time +=
    (tbb::tick_count::now() - push_updates_start).seconds();
}

void ClientLib::reclaim_oplog(
    uint channel_id, iter_t start_clock, iter_t end_clock, uint table_id,
    bool gpu) {
  CommunicationChannel& comm_channel = comm_channels[channel_id];
  CHECK_LT(table_id, comm_channel.cached_tables.size());
  CachedTable& cached_table = comm_channel.cached_tables[table_id];
  ParamCache& param_cache = gpu ?
      cached_table.param_cache_gpu :
      cached_table.param_cache_cpu;
  if (param_cache.oplog.size()) {
    for (iter_t clock = start_clock; clock <= end_clock; clock++) {
      if (clock < 0) {
        continue;
      }
      CHECK_LT(clock, param_cache.oplog.size());
      FlatOps *oplog_entry_to_remove
          = param_cache.oplog[clock];
      if (oplog_entry_to_remove != NULL) {
        oplog_entry_to_remove->reset();
        param_cache.opmem_buffer_pool.put(oplog_entry_to_remove);
        param_cache.oplog[clock] = NULL;
      }
    }
  }
}

void ClientLib::read_batch_local(
    RowData *buffer, OpInfo& op_info,
    cudaStream_t& cuda_stream, RowData *cpu_buffer) {
  CHECK(buffer);
  CHECK(cpu_buffer);
  CHECK(op_info.local_storage_ptr);
  CHECK_EQ(op_info.type, OpInfo::READ);
  CHECK(op_info.local);
  CHECK_NE(op_info.data_location, DataStorage::GPU);
  size_t size = op_info.num_vals_limit * sizeof(val_t);
  if (op_info.data_location == DataStorage::PINNED_CPU) {
    /* If local storage is in pinned memory, we just need one copying */
    CUDA_CHECK(cudaMemcpyAsync(buffer, op_info.local_storage_ptr,
        size, cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  } else {
    CHECK_EQ(op_info.data_location, DataStorage::CPU);
    /* Copy to pinned CPU memory first */
    memcpy(cpu_buffer, op_info.local_storage_ptr, size);
    /* Copy to GPU buffer */
    CUDA_CHECK(cudaMemcpyAsync(buffer, cpu_buffer,
        size, cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  }
}

void ClientLib::update_batch_local(
    OpInfo& op_info, const RowOpVal *updates,
    cudaStream_t& cuda_stream, RowData *cpu_buffer) {
  CHECK(updates);
  CHECK(cpu_buffer);
  CHECK(op_info.local_storage_ptr);
  CHECK_EQ(op_info.type, OpInfo::READ);
  CHECK(op_info.local);
  CHECK_NE(op_info.data_location, DataStorage::GPU);
  size_t size = op_info.num_vals_limit * sizeof(val_t);
  if (op_info.data_location == DataStorage::PINNED_CPU) {
    /* If local storage is in pinned memory, we just need one copying */
    CUDA_CHECK(cudaMemcpyAsync(op_info.local_storage_ptr, updates,
        size, cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  } else {
    CHECK_EQ(op_info.data_location, DataStorage::CPU);
    /* Copy to pinned CPU memory first */
    CUDA_CHECK(cudaMemcpyAsync(cpu_buffer, updates,
        size, cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
    /* Copy to CPU local storage */
    memcpy(op_info.local_storage_ptr, cpu_buffer, size);
  }
}
