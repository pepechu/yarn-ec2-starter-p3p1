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


/**
 * @brief Describes a policy.
 */
struct policy_t {
public:
  /**
   * The type of policy.
   */
  enum type {
    FIFO_RANDOM = 0,
    FIFO_HETER = 1,
    SJF_HETER = 2,
    CUSTOM = 3,
    UNDEF = 10000000
  };

  /**
   * @brief Converts a policy type to its corresponding string.
   * 
   * @param policy the policy type
   * @return the string representation of the policy type
   */
  static std::string to_string(const policy_t::type policy) {
    switch (policy) {
      case policy_t::FIFO_RANDOM:
        return "FIFO-Random";
      case policy_t::FIFO_HETER:
        return "FIFO-Heterogeneous";
      case policy_t::SJF_HETER:
        return "SJF-Heterogeneous";
      case policy_t::CUSTOM:
        return "Customized policy";
      default:
        return "UNDEFINED";
    }
  }
};

/* EOF */
