#ifndef __PORTABLE_BYTES_HPP__
#define __PORTABLE_BYTES_HPP__

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

#include <stddef.h>
#include <stdio.h>
#include <zmq.hpp>

#include <vector>
#include <string>
#include <iostream>

#include "common/gpu-util/math_functions.hpp"

using std::cerr;
using std::endl;

typedef void(free_func_t)(void *data, void *hint);

static void empty_free_func(void *data, void *hint) {
}

class PortableBytes {
 public:
  virtual int init() = 0;
  virtual int init_size(size_t size_) = 0;
  virtual int init_data(
      void *data_, size_t size_,
      free_func_t *ffn_ = empty_free_func, void *hint_ = NULL) = 0;
  virtual void *data() = 0;
  virtual size_t size() = 0;

  template<class T>
  void pack(const T& t) {
    size_t data_size = sizeof(T);
    init_size(data_size);
    *(reinterpret_cast<T *>(data())) = t;
  }

  template<class T>
  void unpack(T& t) {
    assert(size() >= sizeof(T));
    t = *(reinterpret_cast<T *>(data()));
  }

  template<class T>
  void pack_vector(const std::vector<T>& vec) {
    size_t data_size = vec.size() * sizeof(T);
    init_size(data_size);
    memcpy(data(), vec.data(), data_size);
  }

  template<class T>
  void unpack_vector(std::vector<T>& vec) {
    size_t vec_size = size() / sizeof(T);
    vec.resize(vec_size);
    memcpy(vec.data(), data(), size());
  }

  void pack_string(const std::string& str) {
    init_size(str.size());
    memcpy(data(), str.data(), str.size());
  }

  void unpack_string(std::string& str) {
    str.assign(reinterpret_cast<char *>(data()), size());
  }

  void pack_memory(const void *buf, size_t size) {
    init_size(size);
    memcpy(data(), buf, size);
  }

  void pack_memory(
      const void *buf0, size_t size0, const void *buf1, size_t size1) {
    init_size(size0 + size1);
    void *dst0 = data();
    if (size0) {
      CHECK(buf0);
      memcpy(dst0, buf0, size0);
    }
    if (size1) {
      CHECK(buf1);
      CHECK_EQ(sizeof(void *), sizeof(unsigned long));
      void *dst1 = reinterpret_cast<void *>(
          reinterpret_cast<unsigned long>(dst0) + size0);
      memcpy(dst1, buf1, size1);
    }
  }

  void unpack_memory(void *buf, size_t size) {
    memcpy(buf, data(), size);
  }

  void pack_gpu_memory(const void *buf, size_t size, cudaStream_t cuda_stream) {
    init_size(size);
    cudaMemcpyAsync(data(), buf, size,
      cudaMemcpyDefault, cuda_stream);
    cudaStreamSynchronize(cuda_stream);
  }
};

#endif  // __PORTABLE_BYTES_HPP__
