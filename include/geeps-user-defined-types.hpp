#ifndef __geeps_user_defined_types_hpp__
#define __geeps_user_defined_types_hpp__

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

#include <stdint.h>

#include <string>
#include <vector>
#include <utility>
#include <limits>

typedef unsigned int uint;

typedef uint8_t command_t;
typedef size_t row_idx_t;
typedef float val_t;
typedef size_t table_id_t;
typedef int iter_t;

typedef std::pair<table_id_t, row_idx_t> TableRow;
typedef struct {
  table_id_t table;
  row_idx_t row;
} table_row_t;

#define ROW_DATA_SIZE 128
struct ArrayData {
  val_t data[ROW_DATA_SIZE];
  void init() {
    for (size_t i = 0; i < ROW_DATA_SIZE; i++) {
      data[i] = 0;
    }
  }
  ArrayData() {
    init();
  }
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version) {
    ar & data;
  }
};

typedef ArrayData RowData;
typedef ArrayData RowOpVal;

#endif  // defined __geeps_user_defined_types_hpp
