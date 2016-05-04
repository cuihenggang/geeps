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
  int pp_policy;
  int local_opt;
  size_t gpu_memory_capacity;
  int mm_warning_level;
      /* 0: no warning
       * 1: guarantee double buffering for thread cache
       * 2: make sure all local data in GPU memory
       * 3: make sure all parameter cache in GPU memory */
  int pinned_cpu_memory;
  int read_my_writes;

  GeePsConfig() :
    num_tables(1),
    tcp_base_port(9090),
    num_comm_channels(1),
    output_dir(""), log_interval(0),
    pp_policy(0), local_opt(1),
    gpu_memory_capacity(std::numeric_limits<size_t>::max()),
    mm_warning_level(1),
    pinned_cpu_memory(1),
    read_my_writes(0) {}
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
