#ifndef __ZMQ_PORTABLE_BYTES_HPP__
#define __ZMQ_PORTABLE_BYTES_HPP__

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
