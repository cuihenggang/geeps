#ifndef CPU_ONLY

#include <math_functions.h>  // CUDA's, not caffe's, for fabs, signbit
#include <thrust/device_vector.h>
#include <thrust/functional.h>  // thrust::plus
#include <thrust/reduce.h>

#include <cmath>
#include <cstdlib>
#include <cstring>

#include "math_functions.hpp"

template <>
void gpu_gemm<float>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const float alpha, const float* A, const float* B, const float beta,
    float* C) {
  // Note that cublas follows fortran order.
  int lda = (TransA == CblasNoTrans) ? K : M;
  int ldb = (TransB == CblasNoTrans) ? N : K;
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  cublasOperation_t cuTransB =
      (TransB == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  CUBLAS_CHECK(cublasSgemm(cublas_handle, cuTransB, cuTransA,
      N, M, K, &alpha, B, ldb, A, lda, &beta, C, N));
}

template <>
void gpu_gemm<double>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const double alpha, const double* A, const double* B, const double beta,
    double* C) {
  // Note that cublas follows fortran order.
  int lda = (TransA == CblasNoTrans) ? K : M;
  int ldb = (TransB == CblasNoTrans) ? N : K;
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  cublasOperation_t cuTransB =
      (TransB == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  CUBLAS_CHECK(cublasDgemm(cublas_handle, cuTransB, cuTransA,
      N, M, K, &alpha, B, ldb, A, lda, &beta, C, N));
}

template <>
void gpu_gemm<float>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const float alpha, const float* A, const int lda,
    const float* B, const int ldb,
    const float beta, float* C, const int ldc) {
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  cublasOperation_t cuTransB =
      (TransB == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  CUBLAS_CHECK(cublasSgemm(cublas_handle, cuTransB, cuTransA,
      N, M, K, &alpha, B, ldb, A, lda, &beta, C, ldc));
}

template <>
void gpu_gemm<double>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const double alpha, const double* A, const int lda,
    const double* B, const int ldb,
    const double beta, double* C, const int ldc) {
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  cublasOperation_t cuTransB =
      (TransB == CblasNoTrans) ? CUBLAS_OP_N : CUBLAS_OP_T;
  CUBLAS_CHECK(cublasDgemm(cublas_handle, cuTransB, cuTransA,
      N, M, K, &alpha, B, ldb, A, lda, &beta, C, ldc));
}

template <>
void gpu_gemv<float>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA, const int M,
    const int N, const float alpha, const float* A, const float* x,
    const float beta, float* y) {
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_T : CUBLAS_OP_N;
  CUBLAS_CHECK(cublasSgemv(cublas_handle, cuTransA, N, M, &alpha,
      A, N, x, 1, &beta, y, 1));
}

template <>
void gpu_gemv<double>(
    cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA, const int M,
    const int N, const double alpha, const double* A, const double* x,
    const double beta, double* y) {
  cublasOperation_t cuTransA =
      (TransA == CblasNoTrans) ? CUBLAS_OP_T : CUBLAS_OP_N;
  CUBLAS_CHECK(cublasDgemv(cublas_handle, cuTransA, N, M, &alpha,
      A, N, x, 1, &beta, y, 1));
}

template <>
void gpu_axpy<float>(
    cublasHandle_t cublas_handle, const int N,
    const float alpha, const float* X, float* Y) {
  CUBLAS_CHECK(cublasSaxpy(cublas_handle, N, &alpha, X, 1, Y, 1));
}

template <>
void gpu_axpy<double>(
    cublasHandle_t cublas_handle, const int N,
    const double alpha, const double* X, double* Y) {
  CUBLAS_CHECK(cublasDaxpy(cublas_handle, N, &alpha, X, 1, Y, 1));
}

template <>
float gpu_dot<float>(cublasHandle_t cublas_handle,
    const int n, const float* x, const float* y) {
  float out;
  CUBLAS_CHECK(cublasSdot(cublas_handle, n, x, 1, y, 1, &out));
  return out;
}

template <>
double gpu_dot<double>(cublasHandle_t cublas_handle,
    const int n, const double* x, const double* y) {
  double out;
  CUBLAS_CHECK(cublasDdot(cublas_handle, n, x, 1, y, 1, &out));
  return out;
}

template <>
float gpu_asum<float>(
    cublasHandle_t cublas_handle, const int n, const float* x) {
  float y;
  CUBLAS_CHECK(cublasSasum(cublas_handle, n, x, 1, &y));
  return y;
}

template <>
double gpu_asum<double>(
    cublasHandle_t cublas_handle, const int n, const double* x) {
  double y;
  CUBLAS_CHECK(cublasDasum(cublas_handle, n, x, 1, &y));
  return y;
}

#endif