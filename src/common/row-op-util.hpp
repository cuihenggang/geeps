#ifndef __row_op_util_hpp__
#define __row_op_util_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <vector>

#include "portable-bytes.hpp"
#include "include/geeps-user-defined-types.hpp"
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
