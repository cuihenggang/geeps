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

#include <utility>
#include <string>
#include <vector>

#include "metadata-server.hpp"

using std::string;
using std::cerr;
using std::cout;
using std::endl;
using std::vector;


void MetadataServer::add_access_info(
      const string& client, uint client_id, const AccessInfo& access_info) {
  if (policy == 3) {
    access_info_received = true;
    for (uint i = 0; i < access_info.size(); i++) {
      const RowAccessInfo& row_access_info = access_info[i];
      TableRow key(row_access_info.tid, row_access_info.rid);
      TargetServer server(
        client_id, row_access_info.nr_read, row_access_info.nr_write);
      if (!tmp_row_tablet_map.count(key)) {
        std::vector<TargetServer> servers;
        servers.push_back(server);
        tmp_row_tablet_map[key] = servers;
      } else {
        std::vector<TargetServer> &cur_servers = tmp_row_tablet_map[key];
        cur_servers.push_back(server);
      }
    }

    nr_access_info_received++;
    if (nr_access_info_received == num_processes) {
      /* Received access info from all clients, now the row-to-tablet mapping
       * should be stable, and we can service FIND_ROW requests
       */
      decide_data_assignment();
      ready_to_serve = true;
      serve_pending_requests();
    }
  }

  /* We also view that as an automatic FIND_ROW request */
  for (uint i = 0; i < access_info.size(); i++) {
    const RowAccessInfo& row_access_info = access_info[i];
    find_row(client, client_id, row_access_info.tid, row_access_info.rid);
  }
}

void MetadataServer::decide_data_assignment() {
  for (boost::unordered_map<TableRow, std::vector<TargetServer> >::iterator
        it = tmp_row_tablet_map.begin();
        it != tmp_row_tablet_map.end(); it++) {
    std::vector<TargetServer> candidates = it->second;
    uint m_ind = 0;
    uint m_freq = 0;
    for (std::vector<TargetServer>::iterator ts_it = candidates.begin();
         ts_it != candidates.end(); ts_it++) {
      uint cur_freq = ts_it->nr_read + ts_it->nr_write;
      if (cur_freq > m_freq ||
        (cur_freq == m_freq &&
        tablet_load[ts_it->tid] < tablet_load[candidates[m_ind].tid])) {
          m_freq = cur_freq;
          m_ind = ts_it - candidates.begin();
      }
    }
    TargetServer chosen_candidate = candidates[m_ind];
    row_tablet_map[it->first] = chosen_candidate;
    tablet_load[chosen_candidate.tid] +=
        chosen_candidate.nr_read + chosen_candidate.nr_write;
  }
}

void MetadataServer::serve_pending_requests() {
  for (uint i = 0; i < pending_requests.size(); i ++) {
    FindRowRequest& request = pending_requests[i];
    TableRow key(request.table, request.row);
    uint server_id = row_tablet_map[key].tid;
    communicator->find_row(
        request.client, request.table, request.row, server_id);
  }
}

uint64_t MetadataServer::get_hash(table_id_t table, row_idx_t row) {
  return row;
}

void MetadataServer::find_row(
    const string& client, uint client_id, table_id_t table, row_idx_t row) {
  server_stats.nr_request++;

  CHECK(0);

  uint server_id = 0;
  TableRow key(table, row);

  switch (policy) {
    case 1:
      /* tablet server <-- row_id % num_processes */
      server_id =
          get_hash(table, row) % (num_processes * num_channels) / num_channels;
      communicator->find_row(client_id, table, row, server_id);
      break;
    case 2:
      /* tablet server <-- first accessing client */
      if (row_tablet_map.find(key) == row_tablet_map.end()) {
        row_tablet_map[key].tid = client_id;
      }
      server_id = row_tablet_map[key].tid;
      communicator->find_row(client_id, table, row, server_id);
      break;
    case 3:
      /* max(local access) + load balancing */
      if (ready_to_serve) {
        RowTableMap::iterator row_tablet_map_it = row_tablet_map.find(key);
        if (row_tablet_map_it != row_tablet_map.end()) {
          server_id = row_tablet_map_it->second.tid;
        } else {
          server_id =
              get_hash(table, row) % (num_processes * num_channels)
                  / num_channels;
        }
        communicator->find_row(client_id, table, row, server_id);
      } else {
        if (access_info_received) {
          /* row-to-tablet mapping is not ready, save to pending requests */
          pending_requests.push_back(FindRowRequest(client_id, table, row));
        } else {
          server_id =
              get_hash(table, row) % (num_processes * num_channels)
                  / num_channels;
          communicator->find_row(client_id, table, row, server_id);
        }
      }
      break;
    default:
      CHECK(0) << "Unknown parameter placement policy: " << policy;
  }
}

string MetadataServer::get_stats() {
  return server_stats.to_json();
}
