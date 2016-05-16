#ifndef __geeps_hpp__
#define __geeps_hpp__

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
