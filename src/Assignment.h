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

#include <memory>
#include <set>
#include <vector>

#include <stdlib.h>
#include "Job.h"

/**
 * @brief Describes an assignment of a job to its corresponding set of machines.
 * @details This is the output of a scheduling decision passed to the policy server handler.
 * Each decision consists of a job and its corresponding machine assignments.
 */
class Assignment {
private:
  const Job job_;
  const std::set<int32_t> machines_;

public:
  /**
   * @brief Creates a job assignment
   *
   * @param job The job
   * @param machines The set of machines assigned to the job
   */
  Assignment(const Job job, const std::set<int32_t> machines) :
      job_(job), machines_(machines) {}

  /**
   * @return the job
   */
  const Job& job() const { return job_; }

  /**
   * @return the set of machines assigned to the job
   */
  const std::set<int32_t>& machines() const { return machines_; }
};
