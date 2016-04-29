/*
 * Copyright (C) 2013 by Carnegie Mellon University.
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
}

void GeePs::thread_start() {
  client_lib->thread_start();
}

void GeePs::thread_stop() {
  client_lib->thread_stop();
}

void GeePs::shutdown() {
  client_lib->shutdown();
}

iter_t GeePs::get_clock() {
  return client_lib->get_clock();
}

string GeePs::json_stats() {
  return client_lib->json_stats();
}

void GeePs::start_opseq() {
  client_lib->start_opseq();
}

bool GeePs::read_batch(
    RowData **buffer_mem_out, int handle, bool stat) {
  return client_lib->read_batch(
      buffer_mem_out, handle, stat);
}

void GeePs::postread_batch(int handle) {
  client_lib->postread_batch(handle);
}

void GeePs::preupdate_batch(
    RowOpVal **updates_mem_ret, int handle, bool stat) {
  client_lib->preupdate_batch(updates_mem_ret, handle, stat);
}

bool GeePs::localaccess_batch(
    RowData **buffer_mem_out, int handle) {
  return client_lib->read_batch(
      buffer_mem_out, handle, false);
}

void GeePs::postlocalaccess_batch(int handle) {
  client_lib->postread_batch(handle);
}

void GeePs::update_batch(int handle) {
  client_lib->update_batch(handle);
}

void GeePs::iterate() {
  client_lib->iterate();
}

int GeePs::virtual_read_batch(
    uint table_id, const vector<row_idx_t>& row_ids,
    iter_t staleness, size_t num_val_limit) {
  return client_lib->virtual_read_batch(
      table_id, row_ids, staleness, num_val_limit);
}

int GeePs::virtual_postread_batch(int prestep_handle) {
  return client_lib->virtual_postread_batch(prestep_handle);
}

int GeePs::virtual_prewrite_batch(
    uint table_id, const vector<row_idx_t>& row_ids, size_t num_val_limit) {
  return client_lib->virtual_prewrite_batch(
      table_id, row_ids, num_val_limit);
}

int GeePs::virtual_write_batch(int prestep_handle) {
  return client_lib->virtual_write_batch(prestep_handle);
}

int GeePs::virtual_localaccess_batch(
    const vector<row_idx_t>& row_ids, size_t num_val_limit, bool fetch) {
  /* table_id doesn't matter for local access */
  uint table_id = 0xdeadbeef;
  return client_lib->virtual_localaccess_batch(
      table_id, row_ids, num_val_limit, fetch);
}

int GeePs::virtual_postlocalaccess_batch(
    int prestep_handle, bool keep) {
  return client_lib->virtual_postlocalaccess_batch(prestep_handle, keep);
}

int GeePs::virtual_clock() {
  return client_lib->virtual_clock();
}

void GeePs::finish_virtual_iteration() {
  client_lib->finish_virtual_iteration();
}
