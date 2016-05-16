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

#include <boost/format.hpp>
#include <boost/make_shared.hpp>

#include <string>
#include <vector>

#include "server-entry.hpp"
#include "server-encoder-decoder.hpp"
#include "tablet-server.hpp"
#include "metadata-server.hpp"

using std::string;
using std::vector;
using std::cerr;
using std::cout;
using std::endl;
using boost::format;
using boost::shared_ptr;
using boost::make_shared;

void ServerThreadEntry::server_entry(
    uint channel_id, uint num_channels,
    uint process_id, uint num_processes,
    shared_ptr<zmq::context_t> zmq_ctx,
    const GeePsConfig& config) {
  uint port = config.tcp_base_port + channel_id;
  string request_url = "tcp://*:" + boost::lexical_cast<std::string>(port);

  /* Init cuda stream and cublas handle */
  cudaStream_t cuda_stream;
  cublasHandle_t cublas_handle;
  CUDA_CHECK(cudaStreamCreate(&cuda_stream));
  CUBLAS_CHECK(cublasCreate(&cublas_handle));
  CUBLAS_CHECK(cublasSetStream(cublas_handle, cuda_stream));

  /* Init communication */
  vector<string> connect_list;   /* Empty connect to */
  vector<string> bind_list;
  bind_list.push_back(request_url);
  string tablet_name = (format("tablet-%i") % process_id).str();
  shared_ptr<RouterHandler> router_handler = make_shared<RouterHandler>(
      channel_id, zmq_ctx, connect_list, bind_list, tablet_name,
      config);

  shared_ptr<ServerClientEncode> encoder = make_shared<ServerClientEncode>(
      router_handler, cuda_stream, cublas_handle,
      num_processes, process_id, config);

  shared_ptr<TabletStorage> storage = make_shared<TabletStorage>(
      channel_id, num_channels, process_id, num_processes,
      encoder, cuda_stream, cublas_handle, config);
  shared_ptr<MetadataServer> metadata_server = make_shared<MetadataServer>(
      channel_id, num_channels, process_id, num_processes,
      encoder, config);
  ClientServerDecode decoder(storage, metadata_server);

  router_handler->do_handler(decoder.get_recv_callback());
}
