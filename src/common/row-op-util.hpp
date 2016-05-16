#ifndef __row_op_util_hpp__
#define __row_op_util_hpp__

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

#include <vector>

#include "geeps-user-defined-types.hpp"
#include "common/portable-bytes.hpp"
#include "common/gpu-util/math_functions.hpp"

struct DoubleIndex {
  size_t id0;
  size_t id1;
  DoubleIndex(size_t id0_i = 0, size_t id1_i = 0) : id0(id0_i), id1(id1_i) {}
};

inline void set_zero(ArrayData& data) {
  data.init();
}

inline void pack_data(PortableBytes& bytes, const ArrayData& data) {
  bytes.pack<ArrayData>(data);
}

inline void unpack_data(ArrayData& data, PortableBytes& bytes) {
  bytes.unpack<ArrayData>(data);
}

inline void operator += (ArrayData& left, const ArrayData& right) {
  for (uint i = 0; i < ROW_DATA_SIZE; i++) {
    left.data[i] += right.data[i];
  }
}

inline void add_row_batch(
    ArrayData *rows_y, const ArrayData *rows_x, size_t batch_size) {
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  size_t n = batch_size * ROW_DATA_SIZE;
  cpu_axpy<val_t>(n, 1, x, y);
}

inline void add_row_batch_gpu(
    cublasHandle_t cublas_handle,
    ArrayData *rows_y, const ArrayData *rows_x, size_t batch_size) {
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  size_t n = batch_size * ROW_DATA_SIZE;
  gpu_axpy<val_t>(cublas_handle, n, 1, x, y);
}

inline void assign_rows_to_double_index_cpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  for (size_t row_index_id = 0; row_index_id < num_rows; row_index_id++) {
    /* Assign rows from "id1" to "id0" */
    size_t row_from = index[row_index_id].id1 + index_offset.id1;
    size_t row_to = index[row_index_id].id0 + index_offset.id0;
    for (size_t val_id = 0; val_id < row_size; val_id++) {
      size_t x_idx = row_from * row_size + val_id;
      size_t y_idx = row_to * row_size + val_id;
      if (y_idx < num_vals_limit) {
        y[y_idx] = x[x_idx];
      }
    }
  }
}

inline void assign_rows_from_double_index_cpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  for (size_t row_index_id = 0; row_index_id < num_rows; row_index_id++) {
    /* Add rows from "id0" to "id1" */
    size_t row_from = index[row_index_id].id0 + index_offset.id0;
    size_t row_to = index[row_index_id].id1 + index_offset.id1;
    for (size_t val_id = 0; val_id < row_size; val_id++) {
      size_t x_idx = row_from * row_size + val_id;
      size_t y_idx = row_to * row_size + val_id;
      if (x_idx < num_vals_limit) {
        y[y_idx] = x[x_idx];
      }
    }
  }      
}

inline void add_rows_from_double_index_cpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit) {
  val_t *y = reinterpret_cast<val_t *>(rows_y);
  const val_t *x = reinterpret_cast<const val_t *>(rows_x);
  for (size_t row_index_id = 0; row_index_id < num_rows; row_index_id++) {
    /* Add rows from "id0" to "id1" */
    size_t row_from = index[row_index_id].id0 + index_offset.id0;
    size_t row_to = index[row_index_id].id1 + index_offset.id1;
    for (size_t val_id = 0; val_id < row_size; val_id++) {
      size_t x_idx = row_from * row_size + val_id;
      size_t y_idx = row_to * row_size + val_id;
      if (x_idx < num_vals_limit) {
        y[y_idx] += x[x_idx];
      }
    }
  }      
}

void assign_rows_to_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream);
void assign_rows_from_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream);
void add_rows_from_double_index_gpu(
    ArrayData *rows_y, const ArrayData *rows_x, const DoubleIndex *index,
    size_t num_rows, DoubleIndex index_offset, size_t row_size,
    size_t num_vals_limit,
    cudaStream_t cuda_stream);

#endif  // defined __row_op_util_hpp__
