#ifndef __work_puller_hpp__
#define __work_puller_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <vector>
#include <string>
#include <iostream>

#include "common/zmq-util.hpp"

class WorkPuller {
  boost::shared_ptr<zmq::context_t> zmq_ctx;
  zmq::socket_t socket;

 public:
  WorkPuller(
      boost::shared_ptr<zmq::context_t> ctx, std::string connection_endpoint);
  int pull_work(uint& cmd_ret, std::vector<ZmqPortableBytes>& args);
};

#endif  // defined __work_puller_hpp__
