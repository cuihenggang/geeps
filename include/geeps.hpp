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
  iter_t log_interval;
  uint pp_policy;
  uint local_opt;
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
    output_dir(""), log_interval(0),
    pp_policy(0), local_opt(1),
    thread_cache_capacity(0), gpu_process_cache_capacity(-3),
    gpu_local_storage_capacity(-3),
    gpu_memory_capacity(-1), read_my_writes(0), pinned_cpu_memory(1) {}
};

class GeePs {
 public:
  GeePs(uint process_id, const GeePsConfig& config);
  void Shutdown();
  std::string GetStats();
  void StartIterations();

  /* Interfaces for virtual iteration */
  int VirtualRead(size_t table_id, const vector<size_t>& row_ids, int slack);
  int VirtualPostRead(int prestep_handle);
  int VirtualPreUpdate(size_t table_id, const vector<size_t>& row_ids);
  int VirtualUpdate(int prestep_handle);
  int VirtualLocalAccess(const vector<size_t>& row_ids, bool fetch);
  int VirtualPostLocalAccess(int prestep_handle, bool keep);
  int VirtualClock();
  void FinishVirtualIteration();

  /* Interfaces for real access */
  bool Read(int handle, RowData **buffer_ptr);
  void PostRead(int handle);
  void PreUpdate(int handle, RowOpVal **buffer_ptr);
  void Update(int handle);
  bool LocalAccess(int handle, RowData **buffer_ptr);
  void PostLocalAccess(int handle);
  void Clock();
};

#endif  // defined __geeps_hpp__
