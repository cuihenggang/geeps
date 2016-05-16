#ifndef __wire_protocol_hpp__
#define __wire_protocol_hpp__

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

// Common data types shared by the client and the server

#include <stdint.h>

#include <vector>

#include "geeps-user-defined-types.hpp"

enum Command {
  FIND_ROW,
  READ_ROW_BATCH,
  CLOCK,
  CLOCK_WITH_UPDATES_BATCH,
  ADD_ACCESS_INFO,
  GET_STATS,
  SHUTDOWN
};
typedef uint8_t command_t;

struct RowAccessInfo {
  table_id_t tid;     /* Table ID */
  row_idx_t rid;      /* Row ID */
  uint32_t nr_read;   /* Read frequency */
  uint32_t nr_write;  /* Write frequency */
};

struct RowKey {
  table_id_t table;
  row_idx_t row;
  RowKey(table_id_t table_i = 0, row_idx_t row_i = 0) :
    table(table_i), row(row_i) {}
};
typedef std::vector<RowKey> RowKeys;

struct cs_find_row_msg_t {
  command_t cmd;
  uint32_t client_id;
  table_id_t table;
  row_idx_t row;
};

struct cs_read_row_batch_msg_t {
  command_t cmd;
  uint32_t client_id;
  iter_t data_age;
  bool prioritized;
};

struct cs_clock_msg_t {
  command_t cmd;
  uint32_t client_id;
  iter_t clock;
  uint32_t table_id;
  int read_branch_id;
};

struct cs_clock_with_updates_batch_msg_t {
  command_t cmd;
  uint32_t client_id;
  iter_t clock;
  uint32_t table_id;
  int update_branch_id;
  int read_branch_id;
};

struct cs_add_access_info_msg_t {
  command_t cmd;
  uint32_t client_id;
};

struct cs_get_stats_msg_t {
  command_t cmd;
  uint32_t client_id;
};


struct sc_clock_msg_t {
  command_t cmd;
  uint32_t server_id;
  iter_t clock;
  uint32_t table_id;
};

struct sc_find_row_msg_t {
  command_t cmd;
  table_id_t table;
  row_idx_t row;
  uint32_t server_id;
};

struct sc_read_row_batch_msg_t {
  command_t cmd;
  uint32_t server_id;
  iter_t data_age;
  iter_t self_clock;
  uint32_t table_id;
  int branch_id;
};

struct sc_get_stats_msg_t {
  command_t cmd;
};

#endif  // defined __wire_protocol_hpp__
