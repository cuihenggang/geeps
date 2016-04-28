#ifndef GPU_UTIL_MATH_FUNCTIONS_H_
#define GPU_UTIL_MATH_FUNCTIONS_H_

#include <stdint.h>
#include <cmath>  // for std::fabs and std::signbit

#include <iostream>

#include "glog/logging.h"

extern "C" {
#include <cblas.h>
}

#include "device_alternate.hpp"
#include "mkl_alternate.hpp"

// Caffe gemm provides a simpler interface to the gemm functions, with the
// limitation that the data has to be contiguous in memory.
template <typename Dtype>
void cpu_gemm(const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const Dtype alpha, const Dtype* A, const Dtype* B, const Dtype beta,
    Dtype* C);

template <typename Dtype>
void cpu_gemm(const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const Dtype alpha, const Dtype* A, const int lda,
    const Dtype* B, const int ldb,
    const Dtype beta, Dtype* C, const int ldc);

template <typename Dtype>
void cpu_gemv(const CBLAS_TRANSPOSE TransA, const int M, const int N,
    const Dtype alpha, const Dtype* A, const Dtype* x, const Dtype beta,
    Dtype* y);

template <typename Dtype>
void cpu_axpy(const int N, const Dtype alpha, const Dtype* X,
    Dtype* Y);

template <typename Dtype>
void cpu_axpby(const int N, const Dtype alpha, const Dtype* X,
    const Dtype beta, Dtype* Y);

template <typename Dtype>
Dtype cpu_dot(const int n, const Dtype* x, const Dtype* y);

template <typename Dtype>
Dtype cpu_strided_dot(const int n, const Dtype* x, const int incx,
    const Dtype* y, const int incy);

// Returns the sum of the absolute values of the elements of vector x
template <typename Dtype>
Dtype cpu_asum(const int n, const Dtype* x);

template <typename Dtype>
void cpu_set(const int N, const Dtype alpha, Dtype *X);

template <typename Dtype>
void cpu_add(const int N, const Dtype* a, const Dtype* b, Dtype* y);

template <typename Dtype>
void cpu_sub(const int N, const Dtype* a, const Dtype* b, Dtype* y);

template <typename Dtype>
void cpu_mul(const int N, const Dtype* a, const Dtype* b, Dtype* y);

template <typename Dtype>
void cpu_div(const int N, const Dtype* a, const Dtype* b, Dtype* y);

template <typename Dtype>
void cpu_powx(const int n, const Dtype* a, const Dtype b, Dtype* y);

template <typename Dtype>
void cpu_sqr(const int N, const Dtype* a, Dtype* y);

template <typename Dtype>
void cpu_exp(const int n, const Dtype* a, Dtype* y);

template <typename Dtype>
void cpu_abs(const int n, const Dtype* a, Dtype* y);

template <typename Dtype>
void cpu_add_scalar(const int N, const Dtype alpha, Dtype* Y);

template <typename Dtype>
void cpu_copy(const int N, const Dtype* X, Dtype* Y);

// Decaf gpu gemm provides an interface that is almost the same as the cpu
// gemm function - following the c convention and calling the fortran-order
// gpu code under the hood.
template <typename Dtype>
void gpu_gemm(cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const Dtype alpha, const Dtype* A, const Dtype* B, const Dtype beta,
    Dtype* C);

template <typename Dtype>
void gpu_gemm(cublasHandle_t cublas_handle, const CBLAS_TRANSPOSE TransA,
    const CBLAS_TRANSPOSE TransB, const int M, const int N, const int K,
    const Dtype alpha, const Dtype* A, const int lda,
    const Dtype* B, const int ldb,
    const Dtype beta, Dtype* C, const int ldc);

template <typename Dtype>
void gpu_gemv(cublasHandle_t cublas_handle,
    const CBLAS_TRANSPOSE TransA, const int M, const int N,
    const Dtype alpha, const Dtype* A, const Dtype* x, const Dtype beta,
    Dtype* y);

template <typename Dtype>
void gpu_axpy(cublasHandle_t cublas_handle, const int N,
    const Dtype alpha, const Dtype* X, Dtype* Y);

template <typename Dtype>
Dtype gpu_dot(
    cublasHandle_t cublas_handle, const int n, const Dtype* x, const Dtype* y);

template <typename Dtype>
Dtype gpu_asum(cublasHandle_t cublas_handle, const int n, const Dtype* x);

#endif  // GPU_UTIL_MATH_FUNCTIONS_H_
