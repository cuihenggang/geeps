#include <math_functions.h>  // CUDA's, not caffe's, for fabs, signbit
#include <thrust/device_vector.h>
#include <thrust/functional.h>  // thrust::plus
#include <thrust/reduce.h>

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
