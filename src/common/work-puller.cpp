/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <string>
#include <vector>

#include "work-puller.hpp"

WorkPuller::WorkPuller(
    boost::shared_ptr<zmq::context_t> ctx, std::string connection_endpoint) :
      zmq_ctx(ctx), socket(*zmq_ctx, ZMQ_PULL) {
  socket.bind(connection_endpoint.c_str());
}

int WorkPuller::pull_work(uint& cmd_ret, std::vector<ZmqPortableBytes>& args) {
  ZmqPortableBytes cmd_str;
  bool more;
  try {
    more = recv_msg(socket, cmd_str);
  } catch(...) {
    std::cerr << "exception in WorkPuller" << std::endl;
    return -1;
  }
  cmd_str.unpack<uint>(cmd_ret);
  cmd_str.close();
  if (more) {
    recv_msgs(socket, args);
  }
  return 0;
}
