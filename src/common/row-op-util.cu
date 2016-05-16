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

#include <iostream>

#include "common/row-op-util.hpp"
#include "common/gpu-util/math_functions.hpp"

using std::cout;
using std::endl;

__global__ void assign_rows_to_double_index_kernel(
    val_t *y, const val_t *x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  CUDA_KERNEL_LOOP(i, num_rows * row_size) {
    size_t row_index_id = i / row_size;
    size_t val_id = i % row_size;
    /* Assign rows from "id1" to "id0" */
    size_t row_from = index[row_index_id].id1 + index_offset.id1;
    size_t row_to = index[row_index_id].id0 + index_offset.id0;
    size_t x_idx = row_from * row_size + val_id;
    size_t y_idx = row_to * row_size + val_id;
    if (y_idx < num_vals_limit) {
      y[y_idx] = x[x_idx];
    }
  }
}

void assign_rows_to_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream) {
  if (num_rows == 0) {
    return;
  }
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  assign_rows_to_double_index_kernel
      <<<gpu_util::GET_BLOCKS(num_rows * row_size),
         gpu_util::CUDA_NUM_THREADS, 0, cuda_stream>>>
      (y, x, index, num_rows, index_offset, row_size, num_vals_limit);
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
}

__global__ void assign_rows_from_double_index_kernel(
    val_t *y, const val_t *x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  CUDA_KERNEL_LOOP(i, num_rows * row_size) {
    size_t row_index_id = i / row_size;
    size_t val_id = i % row_size;
    /* Assign rows from "id0" to "id1" */
    size_t row_from = index[row_index_id].id0 + index_offset.id0;
    size_t row_to = index[row_index_id].id1 + index_offset.id1;
    size_t x_idx = row_from * row_size + val_id;
    size_t y_idx = row_to * row_size + val_id;
    if (x_idx < num_vals_limit) {
      y[y_idx] = x[x_idx];
    }
  }
}

void assign_rows_from_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream) {
  if (num_rows == 0) {
    return;
  }
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  assign_rows_from_double_index_kernel
      <<<gpu_util::GET_BLOCKS(num_rows * row_size),
         gpu_util::CUDA_NUM_THREADS, 0, cuda_stream>>>
      (y, x, index, num_rows, index_offset, row_size, num_vals_limit);
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
}

__global__ void add_rows_from_double_index_kernel(
    val_t *y, const val_t *x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  CUDA_KERNEL_LOOP(i, num_rows * row_size) {
    size_t row_index_id = i / row_size;
    size_t val_id = i % row_size;
    /* Add rows from "id0" to "id1" */
    size_t row_from = index[row_index_id].id0 + index_offset.id0;
    size_t row_to = index[row_index_id].id1 + index_offset.id1;
    size_t x_idx = row_from * row_size + val_id;
    size_t y_idx = row_to * row_size + val_id;
    if (x_idx < num_vals_limit) {
      y[y_idx] += x[x_idx];
    }
  }
}

void add_rows_from_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream) {
  if (num_rows == 0) {
    return;
  }
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  add_rows_from_double_index_kernel
      <<<gpu_util::GET_BLOCKS(num_rows * row_size),
         gpu_util::CUDA_NUM_THREADS, 0, cuda_stream>>>
      (y, x, index, num_rows, index_offset, row_size, num_vals_limit);
  CUDA_CHECK(cudaStreamSynchronize(cuda_stream));
}
