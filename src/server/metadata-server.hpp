#ifndef __metadata_server_hpp__
#define __metadata_server_hpp__

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

#include <vector>
#include <string>
#include <set>
#include <map>

#include <boost/shared_ptr.hpp>

#include "geeps-user-defined-types.hpp"
#include "common/common-util.hpp"
#include "server-encoder-decoder.hpp"

using boost::shared_ptr;

class MetadataServer {
  struct Stats {
    uint policy;
    int64_t nr_request;

    Stats() {
      nr_request = 0;
    }

    Stats& operator += (const Stats& rhs) {
      nr_request += rhs.nr_request;
      return *this;
    }
    std::string to_json() {
      std::stringstream ss;
      ss << "{"
         << "\"policy\": " << policy << ", "
         << "\"nr_request\": " << nr_request
         << " } ";
      return ss.str();
    }
  };

  typedef std::vector<RowAccessInfo> AccessInfo;

  struct TargetServer {
    table_id_t tid;     /* Table ID */
    uint32_t nr_read;   /* Read frequency */
    uint32_t nr_write;  /* Write frequency */

    TargetServer(table_id_t t = 0, uint32_t r = 0, uint32_t w = 0) {
      tid = t;
      nr_read = r;
      nr_write = w;
    }
  };

  struct FindRowRequest {
    uint client;
    table_id_t table;     /* Table ID */
    row_idx_t row;     /* Row ID */

    FindRowRequest(
        uint client_i = 0, table_id_t table_i = 0, row_idx_t row_i = 0) :
          client(client_i), table(table_i), row(row_i) {}
  };

  uint channel_id;
  uint num_channels;
  uint process_id;
  uint num_processes;

  shared_ptr<ServerClientEncode> communicator;
  Stats server_stats;
  std::string log_output_dir;

  bool access_info_received;
  bool ready_to_serve;
  // 1 - tablet server <-- row_id % nr_tablets
  // 2 - tablet server <-- first accessing client
  // 3 - max(local access) + load balancing
  uint policy;
  uint nr_access_info_received;
  std::vector<FindRowRequest> pending_requests;
  std::vector<uint> tablet_load;
  boost::unordered_map<TableRow, std::vector<TargetServer> > tmp_row_tablet_map;
  typedef boost::unordered_map<TableRow, TargetServer> RowTableMap;
  RowTableMap row_tablet_map;

 private:
  void decide_data_assignment();
  void serve_pending_requests();
  uint64_t get_hash(table_id_t table, row_idx_t row);

 public:
  MetadataServer(
      uint channel_id, uint num_channels,
      uint process_id, uint num_processes,
      shared_ptr<ServerClientEncode> communicator,
      const GeePsConfig& config) :
        channel_id(channel_id), num_channels(num_channels),
        process_id(process_id), num_processes(num_processes),
        communicator(communicator),
        log_output_dir(config.output_dir), policy(config.pp_policy),
        tablet_load(num_processes) {
    nr_access_info_received = 0;
    access_info_received = false;
    ready_to_serve = false;
    server_stats.policy = config.pp_policy;
    for (uint i = 0; i < num_processes; i++) {
      tablet_load[i] = 0;
    }
  }
  void add_access_info(
      const std::string& client, uint client_id,
      const AccessInfo& access_info);
  void find_row(
      const std::string& client, uint client_id,
      table_id_t table, row_idx_t row);
  string get_stats();
};

#endif  // defined __metadata_server_hpp__
