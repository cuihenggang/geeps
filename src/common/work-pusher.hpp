#ifndef __work_pusher_hpp__
#define __work_pusher_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/thread.hpp>

#include <vector>
#include <string>

#include "common/zmq-util.hpp"

class WorkPusher {
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  boost::thread_specific_ptr<zmq::socket_t> socket;
  std::string endpoint;

 public:
  WorkPusher(boost::shared_ptr<zmq::context_t> ctx_i,
             std::string connection_endpoint)
    : zmq_ctx(ctx_i), endpoint(connection_endpoint) {}
  void push_work(uint cmd, std::vector<ZmqPortableBytes>& args);
  void push_work(uint cmd);
};

#endif  // defined __work_pusher_hpp__
