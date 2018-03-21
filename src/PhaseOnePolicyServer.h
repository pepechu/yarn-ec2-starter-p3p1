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

#include <stdlib.h>
#include <mutex>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "Policy.h"
#include "Job.h"
#include "SchedulerTransport.h"
#include "PolicyServerIf.h"
#include "PhaseOnePolicyServerHandler.h"

#include "tetrisched_constants.h"
#include "tetrisched_types.h"

#pragma once

/**
 * @brief The policy server used for phase one.
 * Logs each scheduling decision for correctness check.
 */
template<int max_possible_machines>
class PhaseOnePolicyServer : public PolicyServerIf<max_possible_machines> {
private:
  static constexpr const char* OUTPUT_FILE_PATH = "/tmp/phase_one_policy_results.txt";

  // Scheduling decision members
  static constexpr const char* INPUT_STR = "input";
  static constexpr const char* OUTPUT_STR = "output";

  // Scheduling input members
  static constexpr const char* QUEUE_STR = "queue";
  static constexpr const char* MACHINES_STR = "machines";

  // Scheduling input job members
  static constexpr const char* JOB_ID_STR = "jobID";
  static constexpr const char* JOB_TYPE_STR = "jobType";
  static constexpr const char* K_STR = "k";
  static constexpr const char* DURATION_STR = "duration";
  static constexpr const char* SLOW_DURATION_STR = "slowDuration";

  // Job types
  static constexpr const char* MPI_STR = "MPI";
  static constexpr const char* GPU_STR = "GPU";

  const int numMachines_; // number of machines

  std::bitset<max_possible_machines> machineMap_; // The map of available machines.
  std::deque<Job> jobQueue_; // The queue of jobs
  // The handler deciding scheduling of jobs
  PhaseOnePolicyServerHandler<max_possible_machines> policyServerHandler_;
  bool wrote_ = false; // Whether or not we have written to the scheduling decisions file
  std::ofstream logOutputStream_; // The JSON-logging stream

  std::recursive_mutex m_; // mutex for all operations

  static const char* GetJobTypeStr(const Job& job) {
    switch (job.jobType) {
      case job_t::JOB_GPU:
        return GPU_STR;
      case job_t::JOB_MPI:
        return MPI_STR;
      default:
        return nullptr;
    }
  }

  /**
   * @brief Sets the input field of the scheduling decision JSON.
   *
   * @param schedulingInput The JSON of the input
   * @param allocator The JSON document allocator
   */
  void SetInputJson(rapidjson::Value& schedulingInput,
                    rapidjson::Document::AllocatorType& allocator) {
    // Create the queue JSON
    rapidjson::Value queueJson(rapidjson::kArrayType);

    for (const auto& job : jobQueue_) {
      rapidjson::Value jobJson(rapidjson::kObjectType);

      jobJson.AddMember(rapidjson::StringRef(JOB_ID_STR), rapidjson::Value().SetInt(job.jobId), allocator);

      jobJson.AddMember(
        rapidjson::StringRef(JOB_TYPE_STR),
        rapidjson::Value().SetString(rapidjson::StringRef(GetJobTypeStr(job))),
        allocator
      );

      jobJson.AddMember(rapidjson::StringRef(K_STR), rapidjson::Value().SetInt(int(job.k)), allocator);

      jobJson.AddMember(
        rapidjson::StringRef(DURATION_STR), rapidjson::Value().SetInt(int(job.duration)), allocator
      );

      jobJson.AddMember(
        rapidjson::StringRef(SLOW_DURATION_STR), rapidjson::Value().SetInt(int(job.slowDuration)), allocator
      );

      queueJson.PushBack(jobJson, allocator);
    }

    schedulingInput.AddMember(rapidjson::StringRef(QUEUE_STR), queueJson, allocator);

    // Create the machines JSON
    rapidjson::Value machinesJson(rapidjson::kArrayType);

    for (int i = 0; i < numMachines_; i++) {
      if (machineMap_[i] == 0) {
        machinesJson.PushBack(i, allocator);
      }
    }

    schedulingInput.AddMember(rapidjson::StringRef(MACHINES_STR), machinesJson, allocator);
  }

  /**
   * @brief Sets the output field of the scheduling decision JSON.
   *
   * @param assignment The assignment, as decided by the handler
   * @param schedulingOutput The JSON of the output
   * @param allocator The JSON document allocator
   */
  void SetOutputJson(const std::shared_ptr<Assignment> assignment,
                     rapidjson::Value& schedulingOutput,
                     rapidjson::Document::AllocatorType& allocator) {
    if (!assignment) {
      return;
    }

    // Create the Job ID JSON
    schedulingOutput.AddMember(
      rapidjson::StringRef(JOB_ID_STR), rapidjson::Value().SetInt(assignment->job().jobId), allocator
    );

    // Create the machines JSON
    rapidjson::Value machinesJson(rapidjson::kArrayType);
    for (const auto& machine : assignment->machines()) {
      machinesJson.PushBack(machine, allocator);
    }

    schedulingOutput.AddMember(
      rapidjson::StringRef(MACHINES_STR), machinesJson, allocator
    );
  }

  /**
   * @brief Logs the scheduling decision.
   *
   * @param schedulingDecision the JSON document of the scheduling decision
   */
  void LogSchedulingDecision(const rapidjson::Document& schedulingDecision) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    schedulingDecision.Accept(writer);

    if (wrote_) {
      logOutputStream_ << ",\n";
    }

    logOutputStream_ << buffer.GetString();
    logOutputStream_.flush();
    wrote_ = true;
  }

public:
  PhaseOnePolicyServer(const policy_t::type policy,
                       const std::vector<int>& rackCap,
                       const int numLargeMachineRacks,
                       const int numMachines,
                       const SchedulerTransport<max_possible_machines> & schedulerTransport) :
      PolicyServerIf<max_possible_machines>(schedulerTransport),
      numMachines_(numMachines),
      policyServerHandler_(policy, rackCap, numLargeMachineRacks, numMachines) {
    // Erase old output file if exists.
    logOutputStream_.open(OUTPUT_FILE_PATH, std::ios::out | std::ios::trunc);
  }

  /**
   * @brief Calls the server handler schedule function with the job queue and machine map.
   */
  void TrySchedule() {
    std::unique_lock<std::recursive_mutex> lock(m_);

    while (!jobQueue_.empty()) {

      rapidjson::Document scheduleCallJson;
      scheduleCallJson.SetObject();

      // The input object for the scheduling call (queued jobs, and available machines)
      rapidjson::Value input(rapidjson::kObjectType);

      // The output object for the scheduling call ()
      rapidjson::Value output(rapidjson::kObjectType);

      rapidjson::Document::AllocatorType& allocator = scheduleCallJson.GetAllocator();
      SetInputJson(input, allocator);

      // Get the assignment from the handler
      const std::shared_ptr<Assignment> assignment =
          policyServerHandler_.Schedule(jobQueue_, machineMap_);

      SetOutputJson(assignment, output, allocator);
      scheduleCallJson.AddMember(rapidjson::StringRef(INPUT_STR), input, allocator);
      scheduleCallJson.AddMember(rapidjson::StringRef(OUTPUT_STR), output, allocator);

      LogSchedulingDecision(scheduleCallJson);

      if (!this->schedulerTransport_.Schedule(assignment)) {
        return;
      }

      for (auto it = assignment -> machines().begin(); it != assignment -> machines().end(); ++it) {
        machineMap_[*it] = 1;
      }

      // Delete the job from the queue.
      DeleteJob(assignment -> job());
    }
  }

  /**
   * @brief Finds a job in a job queue and removes it from the queue.
   *
   * @param job The job to delete.
   */
  void DeleteJob(const Job &job) {
    std::unique_lock<std::recursive_mutex> lock(m_);

    // delete the job from the job queue
    bool is_erased = false;
    for (std::deque<Job>::iterator it = jobQueue_.begin();
         it != jobQueue_.end(); ++it) {
      if (job.jobId == it->jobId) {
        jobQueue_.erase(it);
        is_erased = true;
        break;
      }
    }

    if (!is_erased) {
      fprintf(stderr, "WARN attempted to remove non-existent job [%d] from the job queue\n", job.jobId);
    }
  }

  /**
   * @brief Adds a new job to the job queue and perform scheduling.
   *
   * @param job The job
   */
  void AddJob(const Job& job) {
    std::unique_lock<std::recursive_mutex> lock(m_);

    jobQueue_.push_back(job);

    TrySchedule();
  }

  /**
   * @brief Mark machines as freed and perform scheduling.
   *
   * @param machines The set of machines freed
   */
  void FreeResources(const std::set<int32_t>& machines) {
    std::unique_lock<std::recursive_mutex> lock(m_);

    for (auto it = machines.begin(); it != machines.end(); ++it) {
      if (machineMap_[*it] == 0) {
        fprintf(stderr, "WARN machine %d is already available and should not be released...\n", int(*it));
      } else {
        machineMap_[*it] = 0;  // Mark machine as free
      }
    }

    TrySchedule();
  }
};
