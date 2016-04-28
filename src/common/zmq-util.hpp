#ifndef __ZMQ_UTIL_HPP__
#define __ZMQ_UTIL_HPP__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

// Helper functions over zmq

#include <boost/format.hpp>
#include <zmq.hpp>

#include <string>
#include <vector>

#include "zmq-portable-bytes.hpp"

using std::vector;
using std::string;

/* The zmq::message_t object is the superclass of zmq_msg_t */
inline void move_pb_to_zmq(zmq::message_t& zmq_msg, ZmqPortableBytes& pb) {
  zmq_msg_t *zmq_msg_ptr = reinterpret_cast<zmq_msg_t *>(&zmq_msg);
  zmq_msg_t *pb_msg_ptr = pb.get_msg_ptr();
  zmq_msg_move(zmq_msg_ptr, pb_msg_ptr);
}

inline void move_zmq_to_pb(ZmqPortableBytes& pb, zmq::message_t& zmq_msg) {
  zmq_msg_t *zmq_msg_ptr = reinterpret_cast<zmq_msg_t *>(&zmq_msg);
  zmq_msg_t *pb_msg_ptr = pb.get_msg_ptr();
  zmq_msg_move(pb_msg_ptr, zmq_msg_ptr);
}

inline void move_string_to_zmq(
    zmq::message_t& zmq_msg, const std::string& str) {
  zmq_msg_t *zmq_msg_ptr = reinterpret_cast<zmq_msg_t *>(&zmq_msg);
  zmq_msg_init_size(zmq_msg_ptr, str.size());
  memcpy(zmq_msg_data(zmq_msg_ptr), str.data(), str.size());
}

inline void move_zmq_to_string(std::string& str, zmq::message_t& zmq_msg) {
  zmq_msg_t *zmq_msg_ptr = reinterpret_cast<zmq_msg_t *>(&zmq_msg);
  str.assign(reinterpret_cast<char *>(
      zmq_msg_data(zmq_msg_ptr)), zmq_msg_size(zmq_msg_ptr));
}

inline bool recv_msg(zmq::socket_t& sock, std::string& msg) {
  zmq::message_t zmq_msg;
  sock.recv(&zmq_msg);
  move_zmq_to_string(msg, zmq_msg);
  int64_t more;
  size_t morelen = sizeof(more);
  sock.getsockopt(ZMQ_RCVMORE, &more, &morelen);
  return (more != 0);
}

inline bool recv_msg(zmq::socket_t& sock, ZmqPortableBytes& msg) {
  zmq::message_t zmq_msg;
  sock.recv(&zmq_msg);
  move_zmq_to_pb(msg, zmq_msg);
  int64_t more;
  size_t morelen = sizeof(more);
  sock.getsockopt(ZMQ_RCVMORE, &more, &morelen);
  return (more != 0);
}

inline bool recv_msgs(zmq::socket_t& sock, vector<ZmqPortableBytes>& msgs) {
  int64_t more = 1;
  while (more) {
    zmq::message_t zmq_msg;
    sock.recv(&zmq_msg);
    if (msgs.size() == msgs.capacity()) {
      /* Enlarge capacity */
      vector<ZmqPortableBytes> tmp_msgs(msgs.size());
      for (uint i = 0; i < msgs.size(); i++) {
        tmp_msgs[i].move(msgs[i]);
      }
      msgs.reserve(tmp_msgs.size() * 2);
      msgs.resize(tmp_msgs.size());
      for (uint i = 0; i < tmp_msgs.size(); i++) {
        msgs[i].move(tmp_msgs[i]);
      }
    }
    msgs.push_back(ZmqPortableBytes());
    move_zmq_to_pb(msgs[msgs.size() - 1], zmq_msg);
    size_t morelen = sizeof(more);
    sock.getsockopt(ZMQ_RCVMORE, &more, &morelen);
  }
  return false;   /* no more messages */
}

inline int send_msg(
    zmq::socket_t& sock, const std::string& data, bool more = false) {
  zmq::message_t zmq_msg;
  move_string_to_zmq(zmq_msg, data);
  if (more) {
    return sock.send(zmq_msg, ZMQ_SNDMORE);
  } else {
    return sock.send(zmq_msg);
  }
}

inline int send_msg(
    zmq::socket_t& sock, ZmqPortableBytes& data, bool more = false) {
  zmq::message_t zmq_msg;
  move_pb_to_zmq(zmq_msg, data);
  if (more) {
    return sock.send(zmq_msg, ZMQ_SNDMORE);
  } else {
    return sock.send(zmq_msg);
  }
}

inline int send_msgs(zmq::socket_t& sock,
                     std::vector<ZmqPortableBytes>& parts,
                     bool more = false) {
  if (parts.size() == 0) {
    return 0;
  }

  int end = parts.size() -1;
  int ret = 0;
  if (more) {
    end = parts.size();
  }
  for (int i = 0; i < end; i++) {
    ret |= send_msg(sock, parts[i], true);
  }
  if (!more) {
    ret |= send_msg(sock, parts[parts.size()-1]);
  }
  return ret;
}

inline void forward_msgs(zmq::socket_t& src, zmq::socket_t& dst) {
  int64_t more = 1;
  size_t morelen = sizeof(more);
  while (more) {
    zmq::message_t zmq_msg;
    /* Process all parts of the message */
    src.recv(&zmq_msg);
    src.getsockopt(ZMQ_RCVMORE, &more, &morelen);
     if (more) {
       dst.send(zmq_msg, ZMQ_SNDMORE);
     } else {
      dst.send(zmq_msg);
    }
  }
}

#endif  // __ZMQ_UTIL_HPP__
