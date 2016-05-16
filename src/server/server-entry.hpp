#ifndef __server_entry_hpp__
#define __server_entry_hpp__

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

#include <boost/shared_ptr.hpp>

#include "common/router-handler.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::cout;
using std::endl;
using boost::shared_ptr;

class ServerThreadEntry {
  uint channel_id;
  uint num_channels;
  uint process_id;
  uint num_processes;
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  GeePsConfig config;

 public:
  ServerThreadEntry(
      uint channel_id, uint num_channels,
      uint process_id, uint num_processes,
      boost::shared_ptr<zmq::context_t> zmq_ctx,
      const GeePsConfig& config) :
        channel_id(channel_id), num_channels(num_channels),
        process_id(process_id), num_processes(num_processes),
        zmq_ctx(zmq_ctx),
        config(config) {
  }

  void operator()() {
    server_entry(
        channel_id, num_channels, process_id, num_processes, zmq_ctx, config);
  }

 private:
  void server_entry(
      uint channel_id, uint num_channels,
      uint process_id, uint num_processes,
      shared_ptr<zmq::context_t> zmq_ctx,
      const GeePsConfig& config);
};

#endif  // defined __server_entry_hpp__
