#ifndef __server_entry_hpp__
#define __server_entry_hpp__

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
