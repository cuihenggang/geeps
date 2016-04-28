#ifndef __geeps_hpp__
#define __geeps_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <string>
#include <vector>

#include "geeps-user-defined-types.hpp"

using std::string;
using std::vector;

struct GeePsConfig {
  uint num_tables;
  std::vector<std::string> host_list;
  std::vector<uint> port_list;
  uint tcp_base_port;
  uint num_comm_channels;
  std::string output_dir;
  iter_t start_clock;
  iter_t snapshot_interval;
  iter_t log_interval;
  uint pp_policy;
  uint local_opt;
  uint static_cache;
  int thread_cache_capacity;
  int gpu_process_cache_capacity;
  int gpu_local_storage_capacity;
  int gpu_memory_capacity;
  int read_my_writes;
  int pinned_cpu_memory;

  GeePsConfig() :
    num_tables(1),
    tcp_base_port(9090),
    num_comm_channels(1),
    output_dir(""),
    start_clock(0), snapshot_interval(0), log_interval(0),
    pp_policy(0), local_opt(1),
    static_cache(1),
    thread_cache_capacity(-1), gpu_process_cache_capacity(-1),
    gpu_local_storage_capacity(-1),
    gpu_memory_capacity(-1), read_my_writes(0), pinned_cpu_memory(0) {}
};

class GeePs {
 public:
  GeePs(uint process_id, const GeePsConfig& config);

  /* Interfaces for IterStore */
  std::string json_stats();
  void thread_start();
  void thread_stop();
  void shutdown();
  iter_t get_clock();
  void iterate();
  int virtual_read_batch(
      uint table_id, const vector<row_idx_t>& row_ids, iter_t staleness,
      size_t num_vals_limit = std::numeric_limits<size_t>::max());
  int virtual_postread_batch(int prestep_handle);
  int virtual_prewrite_batch(
      uint table_id, const vector<row_idx_t>& row_ids,
      size_t num_vals_limit = std::numeric_limits<size_t>::max());
  int virtual_write_batch(int prestep_handle);
  int virtual_localaccess_batch(
      const vector<row_idx_t>& row_ids, size_t num_vals_limit, bool fetch);
  int virtual_postlocalaccess_batch(int prestep_handle, bool keep);
  int virtual_clock();
  void finish_virtual_iteration();

  /* Interfaces for GeePS */
  void start_opseq();
  bool read_batch(
      RowData **buffer_mem_ret, int handle, bool timing = true);
  void postread_batch(int handle);
  void preupdate_batch(RowOpVal **updates_mem_ret, int handle, bool stat = true);
  void update_batch(int handle);
  bool localaccess_batch(RowData **buffer_mem_ret, int handle);
  void postlocalaccess_batch(int handle);
};

#endif  // defined __geeps_hpp__
