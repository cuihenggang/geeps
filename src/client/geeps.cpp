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

#include <string>
#include <vector>

#include "geeps.hpp"
#include "clientlib.hpp"

using std::string;
using std::vector;
using boost::shared_ptr;

GeePs::GeePs(
    uint process_id, const GeePsConfig& config) {
  ClientLib::CreateInstance(process_id, config);
  client_lib->thread_start();
}

void GeePs::Shutdown() {
  client_lib->thread_stop();
  client_lib->shutdown();
}

string GeePs::GetStats() {
  return client_lib->json_stats();
}

void GeePs::StartIterations() {
  client_lib->start_opseq();
}

int GeePs::VirtualRead(
    size_t table_id, const vector<size_t>& row_ids, iter_t slack) {
  size_t num_val_limit = row_ids.size() * ROW_DATA_SIZE;
  return client_lib->virtual_read_batch(
      table_id, row_ids, slack, num_val_limit);
}

int GeePs::VirtualPostRead(int prestep_handle) {
  return client_lib->virtual_postread_batch(prestep_handle);
}

int GeePs::VirtualPreUpdate(size_t table_id, const vector<size_t>& row_ids) {
  size_t num_val_limit = row_ids.size() * ROW_DATA_SIZE;
  return client_lib->virtual_prewrite_batch(
      table_id, row_ids, num_val_limit);
}

int GeePs::VirtualUpdate(int prestep_handle) {
  return client_lib->virtual_write_batch(prestep_handle);
}

int GeePs::VirtualLocalAccess(const vector<size_t>& row_ids, bool fetch) {
  /* table_id doesn't matter for local access */
  size_t table_id = 0xdeadbeef;
  size_t num_val_limit = row_ids.size() * ROW_DATA_SIZE;
  return client_lib->virtual_localaccess_batch(
      table_id, row_ids, num_val_limit, fetch);
}

int GeePs::VirtualPostLocalAccess(int prestep_handle, bool keep) {
  return client_lib->virtual_postlocalaccess_batch(prestep_handle, keep);
}

int GeePs::VirtualClock() {
  return client_lib->virtual_clock();
}

bool GeePs::Read(int handle, RowData **buffer_ptr) {
  bool stat = true;
  return client_lib->read_batch(
      buffer_ptr, handle, stat);
}

void GeePs::PostRead(int handle) {
  client_lib->postread_batch(handle);
}

void GeePs::PreUpdate(int handle, RowData **buffer_ptr) {
  bool stat = true;
  client_lib->preupdate_batch(buffer_ptr, handle, stat);
}

bool GeePs::LocalAccess(int handle, RowData **buffer_ptr) {
  bool stat = false;
  return client_lib->read_batch(
      buffer_ptr, handle, stat);
}

void GeePs::PostLocalAccess(int handle) {
  client_lib->postread_batch(handle);
}

void GeePs::Update(int handle) {
  client_lib->update_batch(handle);
}

void GeePs::Clock() {
  client_lib->iterate();
}

void GeePs::FinishVirtualIteration() {
  client_lib->finish_virtual_iteration();
}
