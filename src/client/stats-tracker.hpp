#ifndef __client_stats_hpp__
#define __client_stats_hpp__

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

// Performance tracking and reporting

#include <stdint.h>

#include <string>

#include "common/internal-config.hpp"

using std::string;

struct Stats {
  int64_t nr_threads;
  int64_t row_count;
  int64_t row_count_local;
  int64_t nr_thread_hit;
  int64_t nr_proc_hit;
  int64_t nr_miss;
  int64_t nr_prefetch_miss;
  int64_t nr_proc_hit2;
  int64_t nr_miss2;
  int64_t nr_prefetch_miss2;
  double tot_miss_latency;
  int64_t nr_refresh;
  int64_t nr_read_requests;
  int64_t nr_recv_row;
  int64_t nr_non_pending_respond;
  double tot_non_pending_respond_time;
  double ave_non_pending_respond_time;
  double max_non_pending_respond_time;
  double min_non_pending_respond_time;
  uint32_t prefetch;
  double psafe;
  uint32_t local_opt;
  uint32_t pp_policy;
  uint32_t affinity;

  double iterate_time;
  double iter_flush_log_time;
  double iter_var_time;

  double app_read_time;
  double app_postread_time;
  double app_prewrite_time;
  double app_write_time;

  double bg_read_time;
  double bg_read_read_time;
  double bg_read_move_time;
  double bg_postread_time;
  double bg_prewrite_time;
  double bg_write_time;
  double bg_write_move_time;
  double bg_write_update_time;
  double bg_clock_time;

  double vi_time;

  int64_t nr_cache_copy;
  int64_t tot_read;
  int64_t tot_staleness;
  int64_t tot_increment;
  int64_t tot_apply_thr_cache;
  int64_t tot_apply_proc_cache;
  int64_t nr_thread_cache;
  int64_t nr_thread_cache_max;
  int64_t max_tot_access;
  uint bucket_count;
  uint max_bucket_size;
  uint tot_bucket_size;

  string router_stats;
  string bgthread_stats;
  bool server_stats_refreshed;
  string server_stats;

  void reset() {
    nr_thread_hit = 0;
    nr_proc_hit = 0;
    nr_miss = 0;
    nr_prefetch_miss = 0;
    nr_proc_hit2 = 0;
    nr_miss2 = 0;
    nr_prefetch_miss2 = 0;
    tot_miss_latency = 0.0;
    nr_refresh = 0;
    nr_read_requests = 0;
    nr_recv_row = 0;
    nr_non_pending_respond = 0;
    tot_non_pending_respond_time = 0.0;
    ave_non_pending_respond_time = 0.0;
    max_non_pending_respond_time = 0.0;
    min_non_pending_respond_time = 1000;

    iterate_time = 0.0;
    iter_flush_log_time = 0.0;
    iter_var_time = 0.0;
    nr_cache_copy = 0;
    tot_read = 0;
    tot_staleness = 0;
    tot_increment = 0;
    tot_apply_thr_cache = 0;
    tot_apply_proc_cache = 0;

    app_read_time = 0;
    app_postread_time = 0;
    app_prewrite_time = 0;
    app_write_time = 0;

    bg_read_time = 0;
    bg_read_read_time = 0;
    bg_read_move_time = 0;
    bg_postread_time = 0;
    bg_prewrite_time = 0;
    bg_write_time = 0;
    bg_write_move_time = 0;
    bg_write_update_time = 0;
    bg_clock_time = 0;
  }

  Stats() {
    nr_threads = 0;
    nr_thread_cache = 0;
    nr_thread_cache_max = 0;
    bucket_count = 0;
    max_bucket_size = 0;
    tot_bucket_size = 0;
    vi_time = 0;
    reset();
  }

  Stats& operator += (const Stats& rhs) {
    nr_thread_hit += rhs.nr_thread_hit;
    nr_proc_hit += rhs.nr_proc_hit;
    nr_miss += rhs.nr_miss;
    nr_prefetch_miss += rhs.nr_prefetch_miss;
    nr_proc_hit2 += rhs.nr_proc_hit2;
    nr_miss2 += rhs.nr_miss2;
    nr_prefetch_miss2 += rhs.nr_prefetch_miss2;
    tot_miss_latency += rhs.tot_miss_latency;
    nr_refresh += rhs.nr_refresh;
    nr_read_requests += rhs.nr_read_requests;
    nr_recv_row += rhs.nr_recv_row;

    iterate_time += rhs.iterate_time;
    iter_flush_log_time += rhs.iter_flush_log_time;

    vi_time += rhs.vi_time;

    nr_cache_copy += rhs.nr_cache_copy;
    tot_read += rhs.tot_read;
    tot_staleness += rhs.tot_staleness;
    tot_increment += rhs.tot_increment;
    tot_apply_thr_cache += rhs.tot_apply_thr_cache;
    tot_apply_proc_cache += rhs.tot_apply_proc_cache;
    nr_thread_cache += rhs.nr_thread_cache;
    nr_thread_cache_max =
        rhs.nr_thread_cache_max > nr_thread_cache_max ?
          rhs.nr_thread_cache_max : nr_thread_cache_max;
    bucket_count += rhs.bucket_count;
    max_bucket_size += rhs.max_bucket_size;
    tot_bucket_size += rhs.tot_bucket_size;
    return *this;
  }

  std::string to_json() {
    std::stringstream ss;
    ss << "{ "
       << "\"nr_threads\": " << nr_threads << ", "
       << "\"row_count\": " << row_count << ", "
       << "\"row_count_local\": " << row_count_local << ", "
       << "\"prefetch\": " << prefetch << ", "
       << "\"psafe\": " << psafe << ", "
       << "\"local_opt\": " << local_opt << ", "
       << "\"pp_policy\": " << pp_policy << ", "
       << "\"affinity\": " << affinity << ", "
       << "\"nr_thread_cache\": " << nr_thread_cache << ", "
       << "\"nr_thread_cache_max\": " << nr_thread_cache_max << ", "
       << "\"bucket_count\": " << bucket_count << ", "
       << "\"max_bucket_size\": " << max_bucket_size << ", "
       << "\"tot_bucket_size\": " << tot_bucket_size << ", "
       << "\"nr_thread_hit\": " << nr_thread_hit << ", "
       << "\"nr_proc_hit\": " << nr_proc_hit << ", "
       << "\"nr_miss\": " << nr_miss << ", "
       << "\"nr_prefetch_miss\": " << nr_prefetch_miss << ", "
       << "\"nr_proc_hit2\": " << nr_proc_hit2 << ", "
       << "\"nr_miss2\": " << nr_miss2 << ", "
       << "\"nr_prefetch_miss2\": " << nr_prefetch_miss2 << ", "
       << "\"nr_refresh\": " << nr_refresh << ", "
       << "\"nr_read_requests\": " << nr_read_requests << ", "
       << "\"nr_recv_row\": " << nr_recv_row << ", "
       << "\"nr_non_pending_respond\": " << nr_non_pending_respond << ", "
       << "\"ave_non_pending_respond_time\": "
       << ave_non_pending_respond_time << ", "
       << "\"min_non_pending_respond_time\": "
       << min_non_pending_respond_time << ", "
       << "\"max_non_pending_respond_time\": "
       << max_non_pending_respond_time << ", "
       << "\"READ_TIMING_FREQ\": " << READ_TIMING_FREQ << ", "

       << "\"iterate_time\": " << iterate_time / nr_threads << ", "
       << "\"iter_flush_log_time\": "
       << iter_flush_log_time / nr_threads << ", "
       << "\"iter_var_time\": " << iter_var_time << ", "
 
       << "\"app_read_time\": " << app_read_time << ", "
       << "\"app_postread_time\": " << app_postread_time << ", "
       << "\"app_prewrite_time\": " << app_prewrite_time << ", "
       << "\"app_write_time\": " << app_write_time << ", "

       << "\"bg_read_time\": " << bg_read_time << ", "
       << "\"bg_read_read_time\": " << bg_read_read_time << ", "
       << "\"bg_read_move_time\": " << bg_read_move_time << ", "
       << "\"bg_postread_time\": " << bg_postread_time << ", "
       << "\"bg_prewrite_time\": " << bg_prewrite_time << ", "
       << "\"bg_write_time\": " << bg_write_time << ", "
       << "\"bg_write_move_time\": " << bg_write_move_time << ", "
       << "\"bg_write_update_time\": " << bg_write_update_time << ", "
       << "\"bg_clock_time\": " << bg_clock_time << ", "
       << "\"bg_total_time\": "
          << bg_read_read_time + bg_read_move_time + bg_postread_time +
             bg_prewrite_time + bg_write_move_time + bg_write_update_time +
             bg_clock_time << ", "

       << "\"vi_time\": " << vi_time / nr_threads << ", "

       << "\"nr_cache_copy\": " << nr_cache_copy << ", "
       << "\"tot_read\": " << tot_read << ", "
       << "\"tot_staleness\": " << tot_staleness << ", "
       << "\"tot_increment\": " << tot_increment << ", "
       << "\"tot_apply_thr_cache\": " << tot_apply_thr_cache << ", "
       << "\"tot_apply_proc_cache\": " << tot_apply_proc_cache << ", "

       << "\"router_stats\": " << router_stats << ", "
       << "\"bgthread_stats\": " << bgthread_stats << ", "
       << "\"server_stats\": " << server_stats << ", "

       << "\"last_entry\": 0"
       << "}";
    return ss.str();
  }
};  // end of struct Stats

struct BgthreadStats {
  int64_t tot_recv_row;
  int64_t recv_row_nr_apply_oplog;
  double tot_recv_row_time;
  double recv_row_get_lock_time;
  double recv_row_get_memory_time;
  double recv_row_erase_fetch_time;
  double recv_row_erase_oplog_time;
  double recv_row_copy_data_time;
  double recv_row_apply_oplog_time;

  double tot_push_updates_time;
  double push_updates_get_global_lock_time;
  double push_updates_find_row_time;
  double push_updates_get_lock_time;
  double push_updates_send_update_time;
  double push_updates_send_iterate_time;
  iter_t push_updates_iter;
  int64_t push_updates_count;

  void reset() {
    tot_recv_row = 0;
    recv_row_nr_apply_oplog = 0;
    tot_recv_row_time = 0.0;
    recv_row_get_lock_time = 0.0;
    recv_row_get_memory_time = 0.0;
    recv_row_erase_fetch_time = 0.0;
    recv_row_erase_oplog_time = 0.0;
    recv_row_copy_data_time = 0.0;
    recv_row_apply_oplog_time = 0.0;

    tot_push_updates_time = 0.0;
    push_updates_get_global_lock_time = 0.0;
    push_updates_find_row_time = 0.0;
    push_updates_get_lock_time = 0.0;
    push_updates_send_update_time = 0.0;
    push_updates_send_iterate_time = 0.0;
    push_updates_count = 0;
  }

  BgthreadStats() {
    reset();
  }

  BgthreadStats& operator += (const BgthreadStats& rhs) {
    tot_recv_row += rhs.tot_recv_row;
    recv_row_nr_apply_oplog += rhs.recv_row_nr_apply_oplog;
    tot_recv_row_time += rhs.tot_recv_row_time;
    recv_row_get_lock_time += rhs.recv_row_get_lock_time;
    recv_row_get_memory_time += rhs.recv_row_get_memory_time;
    recv_row_erase_fetch_time += rhs.recv_row_erase_fetch_time;
    recv_row_erase_oplog_time += rhs.recv_row_erase_oplog_time;
    recv_row_copy_data_time += rhs.recv_row_copy_data_time;
    recv_row_apply_oplog_time += rhs.recv_row_apply_oplog_time;
    tot_push_updates_time += rhs.tot_push_updates_time;
    push_updates_get_global_lock_time += rhs.push_updates_get_global_lock_time;
    push_updates_find_row_time += rhs.push_updates_find_row_time;
    push_updates_get_lock_time += rhs.push_updates_get_lock_time;
    push_updates_send_update_time += rhs.push_updates_send_update_time;
    push_updates_send_iterate_time += rhs.push_updates_send_iterate_time;
    push_updates_count += rhs.push_updates_count;
    return *this;
  }

  BgthreadStats& operator /= (int n) {
    tot_recv_row /= n;
    recv_row_nr_apply_oplog /= n;
    tot_recv_row_time /= n;
    recv_row_get_lock_time /= n;
    recv_row_get_memory_time /= n;
    recv_row_erase_fetch_time /= n;
    recv_row_erase_oplog_time /= n;
    recv_row_copy_data_time /= n;
    recv_row_apply_oplog_time /= n;
    tot_push_updates_time /= n;
    push_updates_get_global_lock_time /= n;
    push_updates_find_row_time /= n;
    push_updates_get_lock_time /= n;
    push_updates_send_update_time /= n;
    push_updates_send_iterate_time /= n;
    push_updates_count /= n;
    return *this;
  }

  string to_json() {
    std::stringstream ss;
    ss << "{"
       << "\"SET_ROW_TIMING_FREQ\": " << SET_ROW_TIMING_FREQ << ", "
       << "\"tot_recv_row\": " << tot_recv_row << ", "
       << "\"recv_row_nr_apply_oplog\": " << recv_row_nr_apply_oplog << ", "
       << "\"tot_recv_row_time\": "
       << tot_recv_row_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_get_lock_time\": "
       << recv_row_get_lock_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_get_memory_time\": "
       << recv_row_get_memory_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_erase_fetch_time\": "
       << recv_row_erase_fetch_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_erase_oplog_time\": "
       << recv_row_erase_oplog_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_copy_data_time\": "
       << recv_row_copy_data_time * SET_ROW_TIMING_FREQ << ", "
       << "\"recv_row_apply_oplog_time\": "
       << recv_row_apply_oplog_time * SET_ROW_TIMING_FREQ << ", "

       << "\"tot_push_updates_time\": " << tot_push_updates_time << ", "
       << "\"push_updates_get_global_lock_time\": "
       << push_updates_get_global_lock_time << ", "
       << "\"push_updates_get_lock_time\": "
       << push_updates_get_lock_time << ", "
       << "\"push_updates_find_row_time\": "
       << push_updates_find_row_time << ", "
       << "\"push_updates_send_update_time\": "
       << push_updates_send_update_time << ", "
       << "\"push_updates_send_iterate_time\": "
       << push_updates_send_iterate_time << ", "
       << "\"push_updates_iter\": " << push_updates_iter << ", "
       << "\"push_updates_count\": " << push_updates_count << ", "
       << "\"last_entry\": 0"
       << " } ";
    return ss.str();
  }
};  // end of struct BgthreadStats

#endif  // defined __client_stats_hpp__
