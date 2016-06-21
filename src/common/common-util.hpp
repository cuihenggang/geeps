#ifndef __common_util_hpp__
#define __common_util_hpp__

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

#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <iostream>
#include <set>
#include <map>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "geeps-user-defined-types.hpp"
#include "common/wire-protocol.hpp"
#include "common/gpu-util/math_functions.hpp"

using std::vector;
using std::string;
using std::cout;
using std::cerr;
using std::endl;
using boost::unordered_map;

inline iter_t clock_min(vector<iter_t> clocks) {
  CHECK(clocks.size());
  iter_t cmin = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmin = clocks[i] < cmin ? clocks[i] : cmin;
  }
  return cmin;
}

inline iter_t clock_max(vector<iter_t> clocks) {
  CHECK(clocks.size());
  iter_t cmax = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmax = clocks[i] > cmax ? clocks[i] : cmax;
  }
  return cmax;
}

inline uint get_nearest_power2(uint n) {
  uint power2 = 1;
  while (power2 < n) {
    power2 <<= 1;
  }
  return power2;
}

inline void mallocHost(void **ptr, size_t size) {
  /* On CUDA 7.5 and CUDA 7.0, the cudaMallocHost() will sometimes fail
  * even though there is still available memory to allocate.
  * I don't know why it's happening, but as a workaround,
  * I added this while loop to retry cudaMallocHost(). */
  while (cudaMallocHost(ptr, size) != cudaSuccess) {
    cout << "*** WARNING: cudaMallocHost failed, will retry" << endl;
  }
}

inline void mallocHost(RowData **ptr, size_t size) {
  mallocHost(reinterpret_cast<void **>(ptr), size);
}

/* Features:
 * - Single threaded
 * - The entries are freed in the same order as they are allocated. */
struct SimpleCacheHelper {
  size_t size_;
  size_t free_start_;
  size_t free_end_;
  size_t free_count_;
  size_t unused_tail_;
  SimpleCacheHelper() {
    init(0);
  }
  ~SimpleCacheHelper() {
    clear();
  }
  void init(size_t size) {
    size_ = size;
    free_count_ = size_;
    unused_tail_ = size_;
    if (size_) {
      free_start_ = 0;
      free_end_ = size_ - 1;
    }
  }
  void clear() {
    init(0);
  }
  size_t size() {
    return size_;
  }
  size_t get(size_t count, bool wait) {
    CHECK_GE(free_count_, count);
    CHECK_LT(free_start_, size_);
    CHECK_LT(free_end_, size_);
    CHECK_NE(free_start_, free_end_);
    if (free_start_ < free_end_) {
      /* All free space is after free_start_ */
      if (free_start_ + count < free_end_) {
        /* There are enough contiguous free space after free_start_ */
        size_t index = free_start_;
        free_start_ += count;
        free_count_ -= count;
        return index;
      } else {
        cerr << "Insufficient space\n";
        assert(0);
      }
    } else {
      /* There are some free space at the beginning */
      if (free_start_ + count < size_) {
        /* There are enough contiguous free space after free_start_ */
        size_t index = free_start_;
        free_start_ += count;
        free_count_ -= count;
        return index;
      } else {
        /* There are NOT enough contiguous free space after free_start_.
         * We mark the space after free_start_ as unused_tail_,
         * and we go back to the front. */
        unused_tail_ = free_start_;
        free_start_ = 0;
        free_count_ -= (size_ - unused_tail_);
        CHECK_LT(free_start_, free_end_);
        if (free_start_ + count < free_end_) {
          size_t index = free_start_;
          free_start_ += count;
          free_count_ -= count;
          return index;
        } else {
          cerr << "Insufficient space\n";
          assert(0);
        }
      }
    }
  }
  void put(size_t index, size_t count) {
    CHECK_LT(index, size_);
    if (index == (free_end_ + 1) % size_) {
      free_end_ = (free_end_ + count) % size_;
    } else {
      /* There should be an unused tail */
      CHECK_EQ(index, 0);
      CHECK_EQ(unused_tail_, (free_end_ + 1) % size_);
      free_count_ += (size_ - unused_tail_);
      unused_tail_ = size_;
      CHECK_LE(count, size_);
      free_end_ = count - 1;
    }
    free_count_ += count;
  }
};

struct MultithreadedCacheHelper {
  struct AllocMapEntry {
    size_t size;
    int tag;
    AllocMapEntry(size_t size = 0, int tag = 0) : size(size), tag(tag) {}
  };
  typedef std::map<size_t, AllocMapEntry> AllocMap;
  typedef AllocMap::iterator AllocMapIter;
  AllocMap alloc_map_;
  size_t size_;
  size_t allocated_;
  size_t last_alloc_start_;
  boost::mutex mutex_;
  boost::condition_variable cvar_;
  MultithreadedCacheHelper() {
    init(0);
  }
  ~MultithreadedCacheHelper() {
    clear();
  }
  void init(size_t size) {
    size_ = size;
    allocated_ = 0;
    alloc_map_.clear();
    last_alloc_start_ = size_;
      /* Initialize last_alloc_start_ to size_, so that this statement is true:
       *   alloc_map_.find(last_alloc_start_) == alloc_map_.end() */
  }
  void clear() {
    init(0);
  }
  size_t size() {
    return size_;
  }
  size_t get(size_t count, bool wait, int tag = 0) {
    boost::unique_lock<boost::mutex> lock(mutex_);
    while (true) {
      size_t search_start = 0;
      AllocMapIter last_alloc_pos_ = alloc_map_.find(last_alloc_start_);
      if (last_alloc_pos_ != alloc_map_.end()) {
        size_t last_alloc_start = last_alloc_pos_->first;
        size_t last_alloc_count = last_alloc_pos_->second.size;
        /* Search after the last allocated position */
        search_start = last_alloc_start + last_alloc_count;
      }
      size_t start;
      if (search_start < size_) {
        start = search_start;
        for (AllocMapIter map_it = alloc_map_.begin();
             map_it != alloc_map_.end(); map_it++) {
          CHECK_LT(start, size_);
          size_t allocated_start = map_it->first;
          size_t allocated_count = map_it->second.size;
          if (allocated_start < search_start) {
            /* Only search after the last allocated position */
            continue;
          }
          CHECK_LE(start, allocated_start);
          if (start + count <= allocated_start) {
            /* Allocated it before this entry */
            alloc_map_[start] = AllocMapEntry(count, tag);
            last_alloc_start_ = start;
            allocated_ += count;
            return start;
          } else {
            start = allocated_start + allocated_count;
          }
        }
        /* Check the space after the last entry */
        if (start + count <= size_) {
          /* Allocated it at the end */
          alloc_map_[start] = AllocMapEntry(count, tag);
          last_alloc_start_ = start;
          allocated_ += count;
          return start;
        }
      }
      /* Search the space before the last allocated position */
      start = 0;
      for (AllocMapIter map_it = alloc_map_.begin();
           map_it != alloc_map_.end(); map_it++) {
        if (start >= search_start) {
          /* Only search before the last allocated position */
          break;
        }
        CHECK_LT(start, size_);
        size_t allocated_start = map_it->first;
        size_t allocated_count = map_it->second.size;
        CHECK_LE(start, allocated_start);
        if (start + count <= allocated_start) {
          /* Allocated it before this entry */
          alloc_map_[start] = AllocMapEntry(count, tag);
          last_alloc_start_ = start;
          allocated_ += count;
          return start;
        } else {
          start = allocated_start + allocated_count;
        }
      }
      /* If no wait, return size_, indicating there's no more space */
      if (!wait) {
        cerr << "MultithreadedCacheHelper has no more space\n";
        cout << "need " << count << endl;
        cout << "allocated " << allocated_ << endl;
        cout << "size " << size_ << endl;
        print_space();
        return size_;
      }
      /* No more space, wait to be notified */
      // cvar_.wait(lock);
      if (!cvar_.timed_wait(lock,
          boost::posix_time::milliseconds(12000))) {
        cerr << "MultithreadedCacheHelper waits for more space timed out\n";
        cout << "need " << count << endl;
        cout << "allocated " << allocated_ << endl;
        cout << "size " << size_ << endl;
        print_space();
        return size_;
      }
    }
  }
  void put(size_t start, size_t count) {
    boost::unique_lock<boost::mutex> lock(mutex_);
    alloc_map_.erase(start);
    allocated_ -= count;
    cvar_.notify_all();
  }
  void print_space() {
    for (AllocMap::iterator map_it = alloc_map_.begin();
           map_it != alloc_map_.end(); map_it++) {
      size_t allocated_start = map_it->first;
      size_t allocated_count = map_it->second.size;
      int tag = map_it->second.tag;
      cerr << "allocated_start = " << allocated_start << endl;
      cerr << "allocated_count = " << allocated_count << endl;
      cerr << "tag = " << tag << endl;
    }
  }
};

template <typename CacheHelper>
struct GpuCache {
  RowData *data_;
  size_t size_;
  size_t memsize_;
  CacheHelper helper_;
  GpuCache() : helper_() {
    init(0);
  }
  ~GpuCache() {
    clear();
  }
  void init(size_t size) {
    size_ = size;
    memsize_ = size_ * sizeof(RowData);
    data_ = NULL;
    if (memsize_) {
      CUDA_CHECK(cudaMalloc(&data_, memsize_));
    }
    helper_.init(size);
  }
  void clear() {
    if (data_) {
      CUDA_CHECK(cudaFree(data_));
    }
    init(0);
    helper_.clear();
  }
  size_t size() {
    return size_;
  }
  RowData *get(size_t count, bool wait, int tag = 0) {
    size_t index = helper_.get(count, wait, tag);
    if (index >= size_) {
      /* No more space */
      return NULL;
    }
    return &data_[index];
  }
  void put(RowData *buffer, size_t count) {
    size_t index = static_cast<size_t>(buffer - data_);
    helper_.put(index, count);
  }
  void print_space() {
    helper_.print_space();
  }
};

/* TODO: remove row_keys from DataStorage */
struct DataStorage {
  enum MemoryType {
    UNINITIALIZED,
    GPU,
    CPU,
    PINNED_CPU
  } type_;
  size_t size_;
  size_t memsize_;
  std::vector<RowKey> row_keys;
  RowData *ptr_;
  void init(size_t size, MemoryType type) {
    CHECK_EQ(type_, UNINITIALIZED);
    CHECK(!size_);
    CHECK(!memsize_);
    CHECK(!ptr_);
    type_ = type;
    size_ = size;
    memsize_ = size_ * sizeof(RowData);
    row_keys.resize(size_);
    switch (type_) {
      case GPU:
        init_gpu();
        break;
      case CPU:
        init_cpu();
        break;
      case PINNED_CPU:
        init_pinned_cpu();
        break;
      default:
        CHECK_EQ(type_, UNINITIALIZED);
    }
  }
  void init_gpu() {
    CHECK_EQ(type_, GPU);
    if (!memsize_) {
      return;
    }
    CHECK(!ptr_);
    CUDA_CHECK(cudaMalloc(&ptr_, memsize_));
  }
  void init_cpu() {
    CHECK_EQ(type_, CPU);
    if (!memsize_) {
      return;
    }
    CHECK(!ptr_);
    ptr_ = reinterpret_cast<RowData *>(malloc(memsize_));
    CHECK(ptr_);
  }
  void init_pinned_cpu() {
    CHECK_EQ(type_, PINNED_CPU);
    if (!memsize_) {
      return;
    }
    CHECK(!ptr_);
    mallocHost(&ptr_, memsize_);
  }
  void zerofy_data_cpu() {
    CHECK_EQ(type_, CPU);
    if (!memsize_) {
      return;
    }
    CHECK(ptr_);
    memset(ptr_, 0, memsize_);
  }
  void zerofy_data_gpu(cudaStream_t cuda_stream) {
    CHECK_EQ(type_, GPU);
    if (!memsize_) {
      return;
    }
    CHECK(ptr_);
    CHECK(cuda_stream);
    /* We zerofy the data using cudaMemsetAsync() and
     * call cudaStreamSynchronize() after it. */
    CUDA_CHECK(cudaMemsetAsync(ptr_, 0, memsize_, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  }
  size_t size() {
    return size_;
  }
  size_t memsize() {
    return memsize_;
  }
  MemoryType type() {
    return type_;
  }
  RowData *data() {
    if (!memsize_) {
      return NULL;
    }
    CHECK(ptr_);
    return ptr_;
  }
  void init_empty() {
    type_ = UNINITIALIZED;
    size_ = 0;
    memsize_ = 0,
    ptr_ = NULL;
  }
  void init_from(const DataStorage& other) {
    init(other.size_, other.type_);
  }
  void copy(const DataStorage& other) {
    clear();
    type_ = other.type_;
    size_ = other.size_;
    memsize_ = other.memsize_;
    row_keys = other.row_keys;
  }
  void copy_data_gpu(const DataStorage& other, cudaStream_t cuda_stream) {
    CHECK_EQ(type_, GPU);
    CHECK_EQ(other.type_, GPU);
    CHECK_EQ(memsize_, other.memsize_);
    CHECK(cuda_stream);
    CHECK(ptr_);
    CHECK(other.ptr_);
    CUDA_CHECK(cudaMemcpyAsync(
        ptr_, other.ptr_, memsize_, cudaMemcpyDefault, cuda_stream));
    CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
  }
  void copy_data_cpu(const DataStorage& other) {
    CHECK_EQ(type_, CPU);
    CHECK_EQ(other.type_, CPU);
    CHECK_EQ(memsize_, other.memsize_);
    CHECK(ptr_);
    CHECK(other.ptr_);
    memcpy(ptr_, other.ptr_, memsize_);
  }
  void clear() {
    switch (type_) {
      case GPU:
        clear_gpu();
        break;
      case CPU:
        clear_cpu();
        break;
      case PINNED_CPU:
        clear_pinned_cpu();
        break;
      default:
        CHECK_EQ(type_, UNINITIALIZED);
    }
    size_ = 0;
    memsize_ = 0;
    ptr_ = NULL;
    type_ = UNINITIALIZED;
  }
  void clear_gpu() {
    CHECK_EQ(type_, GPU);
    if (ptr_) {
      CUDA_CHECK(cudaFree(ptr_));
    }
  }
  void clear_cpu() {
    CHECK_EQ(type_, CPU);
    if (ptr_) {
      free(ptr_);
    }
  }
  void clear_pinned_cpu() {
    CHECK_EQ(type_, PINNED_CPU);
    if (ptr_) {
      CUDA_CHECK(cudaFreeHost(ptr_));
    }
  }
  DataStorage() {
    init_empty();
  }
  DataStorage(const DataStorage& other) {
    init_empty();
  }
  ~DataStorage() {
    clear();
  }
  DataStorage& operator=(const DataStorage& other) {
    init_empty();
    return *this;
  }
};

#endif  // defined __common_util_hpp__
