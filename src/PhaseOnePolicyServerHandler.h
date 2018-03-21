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

#include <bitset>
#include <deque>

#include "Assignment.h"
#include "Job.h"
#include "Policy.h"
#include "Utility.h"

#include <stdlib.h>

#pragma once

/**
 * @brief The policy server handler that is invoked upon every
 * scheduling decision point (free machine or new job).
 *
 * @details
 * *******************************************************
 * ******* TODO: MODIFY THIS CLASS TO MATCH POLICY *******
 * *******************************************************
 *
 * @tparam max_possible_machines The maximum possible machines the scheduler can handle
 */
template <int max_possible_machines>
class PhaseOnePolicyServerHandler {
private:
  const int numMachines_; // The number of machines in the cluster

public:
  /**
   * @brief The constructor
   *
   * @param policy The policy to use
   * @param rackCap The rack capacity of each rack
   * @param numLargeMachineRacks The number of large machine racks --
   * the first numLargeMachineRacks in rackCap are large machines
   */
  PhaseOnePolicyServerHandler(const policy_t::type policy,
                              const std::vector<int> &rackCap,
                              const int numLargeMachineRacks,
                              const int numMachines) :
      numMachines_(numMachines) {
    if (policy == policy_t::UNDEF) {
       fprintf(stderr, "Unsupported policy %s.", policy_t::to_string(policy).c_str());
       fprintf(stderr, "Exit....");
       exit(1);
    }

    fprintf(stderr, "Using policy: %s\n", policy_t::to_string(policy).c_str());
  }

  /**
   * @brief Generates an assignment of a job to a set of machines upon a new available job or
   * upon free machines.
   * @details Generates an assignment of a job to a set of machines upon a new available job or
   * upon free machines.
   * The job queue should not be modified, while the machineMap can be modified if desired.
   *
   * @param machineMap The map of available machines -- if machineMap[i] == 0, the machine is
   * unassigned, otherwise it has been assigned to some job. machineMap[i] is only
   * valid if i < numMachines_.
   * @return An assignment of a job to a set of machines --
   * should return std::shared_ptr<Assignment>(nullptr) if no valid assignments.
   */
  const std::shared_ptr<Assignment> Schedule(
    const std::deque<Job>& jobQueue, std::bitset<max_possible_machines> machineMap) {
    /******************************************************/
    /***** TODO: MODIFY THIS FUNCTION TO MATCH POLICY *****/
    /******************************************************/
    return std::shared_ptr<Assignment>(nullptr);
  }
};
