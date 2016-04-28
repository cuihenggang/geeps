#ifndef __ZMQ_PORTABLE_BYTES_HPP__
#define __ZMQ_PORTABLE_BYTES_HPP__

/*
 * Copyright(C) 2013 by Carnegie Mellon University.
 *
 */

#include <zmq.h>

#include <algorithm>

#include "portable-bytes.hpp"

class ZmqPortableBytes : public PortableBytes {
 public:
  zmq_msg_t msg;

  ZmqPortableBytes() {
    init();
  }

  int init() {
    return zmq_msg_init(&msg);
  }

  int init_size(size_t size_) {
    return zmq_msg_init_size(&msg, size_);
  }

  int init_data(
      void *data_, size_t size_,
      free_func_t *ffn_ = empty_free_func, void *hint_ = NULL) {
    return zmq_msg_init_data(&msg, data_, size_, ffn_, hint_);
  }

  int copy(ZmqPortableBytes& pb_) {
    return zmq_msg_copy(&msg, pb_.get_msg_ptr());
  }

  int move(ZmqPortableBytes& pb_) {
    return zmq_msg_move(&msg, pb_.get_msg_ptr());
  }

  int close() {
    zmq_msg_close(&msg);
    zmq_msg_init(&msg);
    return 0;
  }

  void *data() {
    return zmq_msg_data(&msg);
  }

  size_t size() {
    return zmq_msg_size(&msg);
  }

  zmq_msg_t *get_msg_ptr() {
    return &msg;
  }

  ~ZmqPortableBytes() {
    // TODO(hengganc): remove this assertion
    assert(!size());
  }

  // /* Disable implicit message copying, so that we won't use shared
  // * messages (less efficient) without being aware of the fact.
  // */
  // ZmqPortableBytes(const ZmqPortableBytes&);
  // void operator = (const ZmqPortableBytes&);
};

#endif  // __ZMQ_PORTABLE_BYTES_HPP__
