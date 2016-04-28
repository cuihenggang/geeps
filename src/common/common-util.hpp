#ifndef __common_util_hpp__
#define __common_util_hpp__

#include <string>
#include <vector>

#include "internal-types.hpp"

using std::string;
using std::vector;
using std::cout;
using std::endl;

inline iter_t clock_min(vector<iter_t> clocks) {
  CHECK(clocks.size());
  iter_t cmin = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmin = clocks[i] < cmin ? clocks[i] : cmin;
  }
  return cmin;
}

inline iter_t clock_max(vector<iter_t> clocks) {
  CHECK(clocks.size());
  iter_t cmax = clocks[0];
  for (uint i = 1; i < clocks.size(); i++) {
    cmax = clocks[i] > cmax ? clocks[i] : cmax;
  }
  return cmax;
}

inline uint get_nearest_power2(uint n) {
  uint power2 = 1;
  while (power2 < n) {
    power2 <<= 1;
  }
  return power2;
}

#endif  // defined __common_util_hpp__
