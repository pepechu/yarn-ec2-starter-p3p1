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

#include "Policy.h"
#include "Job.h"
#include "SchedulerTransport.h"

/**
 * @brief The abstract class for a policy server
 *
 * @tparam max_possible_machines The maximum possible machines the policy server can handle.
 */
template<int max_possible_machines>
class PolicyServerIf {
protected:
  const SchedulerTransport<max_possible_machines> & schedulerTransport_;

public:
  PolicyServerIf(const SchedulerTransport<max_possible_machines> & schedulerTransport) :
    schedulerTransport_(schedulerTransport) {
  }

  virtual ~PolicyServerIf() {}

  /**
   * @brief Registers a job with the policy server.
   *
   * @param job the new job
   */
  virtual void AddJob(const Job& job) = 0;

  /**
   * @brief Frees machines as jobs are completed.
   *
   * @param machines the freed machines
   */
  virtual void FreeResources(const std::set<int32_t>& machines) = 0;
};
