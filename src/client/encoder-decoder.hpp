#ifndef __encoder_decoder_hpp__
#define __encoder_decoder_hpp__

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

// Encode and decode messages to/from the server

#include <vector>
#include <string>

#include "common/wire-protocol.hpp"
#include "common/router-handler.hpp"
#include "common/portable-bytes.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using boost::shared_ptr;

class ClientServerEncode {
  shared_ptr<RouterHandler> router_handler;
  uint num_processes;
  uint client_id;
  vector<string> server_names;

 public:
  ClientServerEncode(
      shared_ptr<RouterHandler> router_handler,
      uint num_processes, uint client_id, const GeePsConfig& config) :
    router_handler(router_handler),
    num_processes(num_processes), client_id(client_id) {
    for (uint i = 0; i < num_processes; i++) {
      std::string server_name("local");
      if (!config.local_opt || i != client_id) {
        server_name = (boost::format("tablet-%i") % i).str();
      }
      server_names.push_back(server_name);
    }
  }
  void clock_broadcast(iter_t clock, uint table_id);
  void clock_with_updates_batch(
      uint server_id, iter_t clock, uint table_id,
      const RowOpVal *updates, const RowKey *row_keys, uint batch_size);
  void clock_with_updates_batch(
      uint server_id, iter_t clock, uint table_id,
      const RowOpVal *updates0, const RowKey *row_keys0, uint batch_size0,
      const RowOpVal *updates1, const RowKey *row_keys1, uint batch_size1);
  void find_row(table_id_t table, row_idx_t row, uint metadata_sever_id);
  void read_row_batch(
      uint server_id, RowKeys& row_keys, iter_t data_age,
      bool prioritized);
  void add_access_info(uint metadata_server_id,
                       const std::vector<RowAccessInfo>& access_info);
  void get_stats(uint server_id);
};

class ServerClientDecode {
  static const uint DECODE_CMD = 1;

  uint channel_id;
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  ClientLib *client_lib;
  bool work_in_background;

  shared_ptr<boost::thread> bg_decode_worker_thread;
  shared_ptr<WorkPusher> decode_work_pusher;

  GeePsConfig config;

 public:
  ServerClientDecode(
      uint channel_id,
      shared_ptr<zmq::context_t> ctx,
      ClientLib *client_lib,
      bool work_in_bg, const GeePsConfig& config);
  ~ServerClientDecode();
  void find_row(vector<ZmqPortableBytes>& args);
  void read_row_batch(vector<ZmqPortableBytes>& args);
  void clock(vector<ZmqPortableBytes>& args);
  void get_stats(vector<ZmqPortableBytes>& args);
  void decode_msg(vector<ZmqPortableBytes>& args);
  void router_callback(const string& src, vector<ZmqPortableBytes>& msgs);
  RouterHandler::RecvCallback get_recv_callback();
  void stop_decoder();
};

#endif  // defined __encoder_decoder_hpp__
