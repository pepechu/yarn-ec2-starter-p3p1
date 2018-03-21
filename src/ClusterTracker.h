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

#include <ios>
#include <fstream>
#include <sstream>
#include <string>
#include <stdlib.h>
#include <ctime>
#include <unordered_set>
#include <unordered_map>
#include <mutex>

#include "Assignment.h"
#include "Job.h"
#include "Utility.h"

namespace {

/**
 * @brief Records job statistics for reporting.
 */
class JobStats {
private:
  int jobId_; // The job ID.
  int64_t submitTime_; // The job submission time.
  int64_t scheduledTime_ = -1; // The job scheduled time.
  int64_t doneTime_ = -1; // The job done time.

public:
  JobStats(const int jobId, const time_t & submitTime) :
    jobId_(jobId), submitTime_(int64_t(submitTime)) {}

  /**
   * @return The job ID.
   */
  int GetJobId() {
    return jobId_;
  }

  /**
   * @return The unix timestamp when a job is submitted
   */
  int64_t GetSubmitTime() {
    return submitTime_;
  }

  /**
   * @brief Sets the scheduled time.
   *
   * @param scheduledTime The unix timestamp when a job is scheduled
   */
  void SetScheduledTime(const time_t & scheduledTime) {
    scheduledTime_ = scheduledTime;
  }

  /**
   * @return The unix timestamp when a job is scheduled
   */
  int64_t GetScheduledTime() {
    return scheduledTime_;
  }

  /**
   * @brief Sets the job as done and marks its time.
   * @details Sets the job as done and marks its time.
   * Note that a job can be KILLED and still be considered "done".
   * The results should be parsed with YARN results.txt.
   *
   * @param doneTime The unix timestamp when a job is done
   */
  void SetDoneTime(const time_t & doneTime) {
    doneTime_ = doneTime;
  }

  /**
   * @return The unix timestamp when a job is done
   */
  int64_t GetDoneTime() {
    return doneTime_;
  }
};
}

/**
 * @brief Tracks cluster and job stats independently of the policy server.
 * Generates job runtime and queueing time results for parsing.
 */
template<int max_possible_machines>
class ClusterTracker {
private:
  const std::string OUTPUT_FILE_PATH = "/tmp/results.txt";

  std::unordered_map<int, std::unordered_set<int32_t>> scheduledJobMap_;
  std::unordered_map<int, std::unordered_set<int32_t>> originalJobAssignmentMap_;
  std::unordered_map<int, JobStats> jobs_;
  std::unordered_map<int, std::string> jobInfoMap_;
  std::bitset<max_possible_machines> machineMap_;
  std::ofstream logStream_;
  const int numMachines_;

  std::recursive_mutex m_;

public:
  ClusterTracker(const std::vector<int> &rackCap) :
      numMachines_(Utility::GetNumMachines(rackCap)) {
    // Erase old output file if exists.
    logStream_.open(OUTPUT_FILE_PATH, std::ios::out | std::ios::trunc);
    logStream_ << "Job ID,Job Type,k,Duration,Slow Duration,Submit Time,Schedule Time,Done Time\n";
  }

  ~ClusterTracker() {
    logStream_.close();
  }

  /**
   * @brief Checks if a job assignment exists and is valid and logs the assignment.
   *
   * @param assignment the job assignment
   * @return if a job assignment exists and is valid
   */
  bool Schedule(const std::shared_ptr<Assignment> assignment) {
    std::unique_lock<std::recursive_mutex> lock(m_);

    // If no assignment, return directly
    if (!assignment) {
      return false;
    }

    // Check if the assignment size is large enough
    if (assignment->job().k > assignment->machines().size()) {
      fprintf(stderr,
        (std::string("WARN did not assign enough machines to job %d! ") +
        "Job requires %d machines but only assigned %d machines! " +
        "Skipping assignment...\n").c_str(),
        int(assignment->job().jobId),
        int(assignment->job().k),
        int(assignment->machines().size())
      );

      return false;
    }

    // Check if the machines are available
    for (auto it = assignment->machines().begin(); it != assignment->machines().end(); ++it) {
      if (machineMap_[*it] == 1) {
        fprintf(stderr,
          "WARN machine %d is not yet available and cannot be used for assignment!\n",
          int(*it)
        );

        return false;
      }

      if (*it >= numMachines_) {
        fprintf(stderr,
          "WARN trying to schedule to machine %d, when there is only a total of %d machines!\n",
          int(*it),
          numMachines_
        );

        return false;
      }
    }

    // Check that jobs have not been scheduled before
    if (scheduledJobMap_.find(assignment->job().jobId) != scheduledJobMap_.end()) {
      fprintf(stderr,
        "WARN job %d has already been scheduled!\n",
        int(assignment->job().jobId)
      );

      return false;
    }

    // Check that the job is actually submitted
    if (jobs_.find(assignment->job().jobId) == jobs_.end()) {
      fprintf(stderr,
        "WARN job %d has not been submitted!\n",
        int(assignment->job().jobId)
      );

      return false;
    }

    // Check that the job has not been scheduled yet
    if (jobs_.find(assignment->job().jobId)->second.GetScheduledTime() > 0) {
      fprintf(stderr,
        "WARN job %d has already been scheduled!\n",
        int(assignment->job().jobId)
      );

      return false;
    }

    // Mark machines as unavailable
    for (auto it = assignment->machines().begin(); it != assignment->machines().end(); ++it) {
      machineMap_[*it] = 1;
    }

    std::unordered_set<int32_t> machines = std::unordered_set<int32_t>(
      assignment->machines().begin(), assignment->machines().end()
    );

    // Insert the schedule
    scheduledJobMap_.emplace(assignment->job().jobId, machines);
    originalJobAssignmentMap_.emplace(assignment->job().jobId, machines);

    // Set the scheduled time as the current unix timestamp
    jobs_.find(assignment->job().jobId)->second.SetScheduledTime(time(NULL));

    return true;
  }

  bool AddJob(const Job job) {
    std::unique_lock<std::recursive_mutex> lock(m_);
    fprintf(stderr, "New JOB %d received\n", job.jobId);

    if (jobs_.find(job.jobId) != jobs_.end()) {
      fprintf(stderr, "Got a duplicate JOB %d, not adding it...\n", job.jobId);
      return false;
    }

    jobs_.emplace(job.jobId, JobStats(job.jobId, time(NULL)));

    std::ostringstream oss;
    oss << int(job.jobType) << "," << int(job.k) << "," << int(job.duration) << "," << int(job.slowDuration);
    std::string jobInfo = oss.str();

    jobInfoMap_.emplace(job.jobId, jobInfo);

    return true;
  }

  /**
   * @brief Frees machines and marks jobs as done.
   *
   * @param machines the machines freed
   */
  std::set<int32_t> FreeResources(const std::set<int32_t>& machines) {
    std::unique_lock<std::recursive_mutex> lock(m_);

    // The set of freed resources
    // Returned only when jobs are done
    std::set<int32_t> freedResources;

    // Free machines
    for (auto machinesIt = machines.begin(); machinesIt != machines.end(); ++machinesIt) {
      if (machineMap_[*machinesIt] == 0) {
        fprintf(
          stderr, "     WARN machine %d is already available and should not be released...\n",
          int(*machinesIt)
        );
      } else {
        machineMap_[*machinesIt] = 0;
      }
    }

    // Mark jobs as done
    auto jobIt = scheduledJobMap_.begin();

    while (jobIt != scheduledJobMap_.end()) {
      // If machine assigned to a job, remove it from the set of the job's machines
      for (auto machinesIt = machines.begin(); machinesIt != machines.end(); ++machinesIt) {
        scheduledJobMap_.find(jobIt->first)->second.erase(*machinesIt);
      }

      // Mark a job as done when all its assigned machines are freed
      if (jobIt->second.empty()) {
        auto jid = jobIt->first;
        auto stats = jobs_.find(jid)->second;

        fprintf(stderr, "JOB %d done!\n", int(jid));
        stats.SetDoneTime(time(NULL));

        auto originalIt = originalJobAssignmentMap_.find(jobIt->first);
        freedResources.insert(originalIt->second.begin(), originalIt->second.end());
        fprintf(stderr, "Free machines -> %d machines...\n", int(freedResources.size()));

        for (auto freedResourcesIt = originalIt->second.begin(); freedResourcesIt != originalIt->second.end(); freedResourcesIt++) {
            fprintf(stderr, " > machine %d\n", int(*freedResourcesIt));
        }

        jobIt = scheduledJobMap_.erase(jobIt);
        originalJobAssignmentMap_.erase(originalIt);

        // Log the job stats to a file and flush it.
        logStream_ << int(jid) << ",";
        logStream_ << jobInfoMap_.find(jid)->second << ",";
        logStream_ << stats.GetSubmitTime() << ",";
        logStream_ << stats.GetScheduledTime() << ",";
        logStream_ << stats.GetDoneTime() << "\n";
        logStream_.flush();
      } else {
        ++jobIt;
      }
    }

    return freedResources;
  }
};
