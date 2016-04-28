#ifndef __geeps_user_defined_types_hpp__
#define __geeps_user_defined_types_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
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
