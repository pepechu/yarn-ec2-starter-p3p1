/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdlib.h>

class Utility {
private:
  Utility() {}
public:
  /**
   * @brief Gets the total number of machines.
   *
   * @param d The JSON document.
   * @return The total number of machines.
   */
  static int GetNumMachines(const std::vector<int>& rackCap) {
    int result = 0;

    for (const auto & cap : rackCap) {
      result += cap;
    }

    return result;
  }

  /**
   * @brief Counts the number of occupied machines.
   *
   * @param machineMap The machine map
   */
  template<int max_possible_machines>
  static int CountUsedMachines(
      const int numMachines, const std::bitset<max_possible_machines> &machineMap) {
    int count = 0;
    for (int i = 0; i < numMachines; ++i) {
      if (machineMap[i] == 1) {
        count++;
      }
    }

    return count;
  }
};

