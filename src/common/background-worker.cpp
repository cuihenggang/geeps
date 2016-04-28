/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

#include <string>
#include <vector>

#include "background-worker.hpp"

int BackgroundWorker::add_callback(uint32_t cmd, WorkerCallback callback) {
  if (cmd == STOP_CMD) {
    return -1;
  }
  if (callback_map.count(cmd)) {
    return -1;
  }

  callback_map[cmd] = callback;
  return 0;
}

void BackgroundWorker::pull_work_loop() {
  while (1) {
    uint32_t cmd = 0;
    std::vector<ZmqPortableBytes> args;
    int ret = work_puller->pull_work(cmd, args);
    if (ret < 0 || cmd == 0) {
      break;
    }
    if (!callback_map.count(cmd)) {
      std::cerr << "Received unknown command!" << std::endl;
      assert(0);
    }
    WorkerCallback& callback = callback_map[cmd];
    callback(args);
  }
}

void BackgroundWorker::operator()() {
  pull_work_loop();
}
