#ifndef __background_worker_hpp__
#define __background_worker_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <boost/make_shared.hpp>
#include <boost/bind.hpp>

#include <vector>
#include <string>

#include "work-puller.hpp"

class BackgroundWorker {
 public:
  explicit BackgroundWorker(const boost::shared_ptr<WorkPuller>& work_puller)
    : work_puller(work_puller) {}
  typedef boost::function<void (std::vector<ZmqPortableBytes>&)>
    WorkerCallback;
  static const uint32_t STOP_CMD = 0;
  int add_callback(uint32_t cmd, WorkerCallback callback);
  void pull_work_loop();
  void operator()();
      /* This function will be used as the entry point of a boost thread */

 private:
  boost::shared_ptr<WorkPuller> work_puller;
  boost::unordered_map<uint32_t, WorkerCallback> callback_map;
};

#endif  // defined __background_worker_hpp__
