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

#include <string>
#include <fstream>
#include <sstream>
#include <bitset>
#include <deque>
#include <iterator>
#include <algorithm>
#include <iostream>

#include "PolicyServerContainer.h"
#include "PhaseOnePolicyServer.h"

#include "PhaseOnePolicyServerHandler.h"

#include "ClusterTracker.h"

#include "YARNTetrischedService.h"
#include "TetrischedService.h"
#include "Assignment.h"
#include "Policy.h"
#include "Utility.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>

#include <rapidjson/document.h>
#include <stdlib.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace ::alsched;

/**
 * @brief The class that communicates with the TetriSched proxy server
 * and initializes the policy server.
 * All scheduling actions go through this entity.
 */
template <int max_possible_machines = 256>
class TetrischedServiceHandler : public TetrischedServiceIf {
private:
  ClusterTracker<max_possible_machines> clusterTracker_;
  SchedulerTransport<max_possible_machines> schedulerTransport_;
  std::shared_ptr<PolicyServerIf<max_possible_machines>> policyServer_;

  /**
   * @brief A factory method that gets the policy server based on the policy specified.
   *
   * @param policy The policy
   * @param numMachines The number of machines available
   * @param schedulerTransport The scheduler transport
   * @return A shared pointer to the policy server
   */
  static std::shared_ptr<PolicyServerIf<max_possible_machines>> getPolicyServer(
      const policy_t::type policy,
      const std::vector<int> & rackCap,
      const int numLargeMachineRacks,
      const int numMachines,
      const SchedulerTransport<max_possible_machines> & schedulerTransport) {
    switch (policy) {
      case policy_t::FIFO_RANDOM:
      case policy_t::FIFO_HETER:
      case policy_t::SJF_HETER:
        return std::shared_ptr<PolicyServerIf<max_possible_machines>>(
          new PhaseOnePolicyServer<max_possible_machines>(
            policy, rackCap, numLargeMachineRacks, numMachines, schedulerTransport
          )
        );

      default:
        // TODO: Implement PhaseTwoPolicyServer for policy_t::Custom
        fprintf(stderr, "Unexpected policy %s", policy_t::to_string(policy).c_str());
        fprintf(stderr, "Exit...");
        exit(1);
    }
  }

public:
  TetrischedServiceHandler(const policy_t::type policy,
                           const std::vector<int>& rackCap,
                           const int numLargeMachineRacks,
                           const int numMachines) :
      clusterTracker_(rackCap),
      schedulerTransport_("r0", 9090, clusterTracker_),
      policyServer_(
        TetrischedServiceHandler<max_possible_machines>::getPolicyServer(
          policy, rackCap, numLargeMachineRacks, numMachines, schedulerTransport_
        )
      ) {

    fprintf(stderr, "[ ");
    std::copy(rackCap.begin(), rackCap.end(), std::ostream_iterator<int>(std::cerr, " "));
    fprintf(stderr, "]\n");

    fprintf(stderr, "== Total num large machine racks: %d\n", numLargeMachineRacks);
    fprintf(stderr, "== Total num machines: %d\n\n", numMachines);
  }

  /**
   * @brief Frees a set of machines.
   *
   * @param machines The set of machine to free.
   */
  virtual void FreeResources(const std::set<int32_t>& machines) override {
    const std::set<int32_t> freedMachines = clusterTracker_.FreeResources(machines);
    if (!freedMachines.empty()) {
      policyServer_->FreeResources(freedMachines);
    }
  }

  /**
   * @brief Adds a job to the job queue and calls the scheduling function.
   *
   * @param jobId The Job ID to add.
   * @param jobType The job type.
   * @param k The number of machines requred for the job to run.
   * @param priority The priority of the job.
   * @param duration How long a job is expected to run for if assigned to preferred resources.
   * @param slowDuration How long a job is expected to run for
   * if not assigned to preferred resources.
   */
  virtual void AddJob(const JobID jobId, const job_t::type jobType,
                      const int32_t k, const int32_t priority,
                      const double duration,
                      const double slowDuration)
                      override {
    Job job;
    job.jobId = int(jobId);
    switch (jobType) {
      case job_t::JOB_GPU:
        job.jobType = Job::JOB_GPU;
        break;
      case job_t::JOB_MPI:
        job.jobType = Job::JOB_MPI;
        break;
      default:
        fprintf(stderr, "Unexpected job type %d\n", int(jobType));
        fprintf(stderr, "Exit...\n");
        exit(1);
    }
    
    job.k = k;
    job.duration = duration;
    job.slowDuration = slowDuration;
    if (clusterTracker_.AddJob(job)) {
      policyServer_->AddJob(job);
    }
  }
};

/**
 * @brief Loads a JSON document.
 *
 * @param jsonf The path to the JSON file.
 * @return A JSON document.
 */
static rapidjson::Document* LoadExternalJsonConfig(const char* jsonf) {
  std::ifstream ifs(jsonf);
  std::stringstream buf;
  buf << ifs.rdbuf();
  std::string input = buf.str();
  rapidjson::Document* d = new rapidjson::Document;
  d->Parse(input.c_str());
  return d;
}

/**
 * @brief Gets the total number of machines.
 *
 * @param d The JSON document.
 * @return The rack capacities
 */
static std::vector<int> GetRackCapacities(rapidjson::Document* d) {
  std::vector<int> rackCapacities;

  // Count the number of machines in each rack.
  const rapidjson::Value& rack_cap = (*d)["rack_cap"];
  for (size_t i = 0; i < rack_cap.Size(); ++i) {
    rackCapacities.push_back(rack_cap[i].GetInt());
  }

  return rackCapacities;
}

/**
 * @brief Gets the number of large machine racks.
 *
 * @param d The JSON document.
 * @return The number of large machine racks
 */
static int GetNumLargeMachineRacks(rapidjson::Document* d) {
  // Count the number of machines in each rack.
  const rapidjson::Value& numLargeMachineRacks = (*d)["numLargeMachineRacks"];
  return numLargeMachineRacks.GetInt();
}

/**
 * @brief Gets the policy
 *
 * @param d The JSON document.
 * @return The policy specified for the policy server.
 */
static policy_t::type GetPolicy(rapidjson::Document* d) {
  policy_t::type policy = policy_t::UNDEF;
  const rapidjson::Value& policy_json = (*d)["policy"];
  std::string policy_str = policy_json.GetString();
  if (policy_str.compare("fifo-r") == 0) {
    policy = policy_t::FIFO_RANDOM;
  } else if (policy_str.compare("fifo-h") == 0) {
    policy = policy_t::FIFO_HETER;
  } else if (policy_str.compare("sjf-h") == 0) {
    policy = policy_t::SJF_HETER;
  } else if (policy_str.compare("custom") == 0) {
    policy = policy_t::CUSTOM;
  }

  if (policy == policy_t::UNDEF) {
    fprintf(stderr, "Unknown policy [%s]\n", policy_str.c_str());
    fprintf(stderr, "Exit...\n");
    exit(1);
  }

  return policy;
}

int main(int argc, char* argv[]) {
  int numMachines = 22;  // 4 large nodes + 18 small ones by default
  policy_t::type policy = policy_t::UNDEF;
  int myPort = 9091;

  // Load json configuration from an external file
  const char* jsonf = getenv("MY_CONFIG");  // exported by run-policy-server.sh
  if (jsonf == NULL) {
    fprintf(
      stderr,
      "%s",
      (std::string("ERR Job configuration file not defined in MY_CONFIG environment variable, ") +
      "was it set up correctly in run-policy-server?\n").c_str()
    );

    fprintf(stderr, "Exit...\n");
    exit(1);
  }

  fprintf(stderr, "Loading json config from %s\n", jsonf);
  auto json = LoadExternalJsonConfig(jsonf);

  const std::vector<int> rackCap = GetRackCapacities(json);
  const int numLargeMachineRacks = GetNumLargeMachineRacks(json);
  numMachines = Utility::GetNumMachines(rackCap);
  policy = GetPolicy(json);
  delete json;

  TetrischedServiceIf* const myhandler = new TetrischedServiceHandler<>(
      policy, rackCap, numLargeMachineRacks, numMachines
  );

  boost::shared_ptr<TetrischedServiceIf>
      handler(myhandler);
  boost::shared_ptr<TProcessor>
      processor(new TetrischedServiceProcessor(handler));
  boost::shared_ptr<TServerTransport>
      serverTransport(new TServerSocket(myPort));
  boost::shared_ptr<TTransportFactory>
      transportFactory(new TBufferedTransportFactory());
  boost::shared_ptr<TProtocolFactory>
      protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor,
      serverTransport,
      transportFactory,
      protocolFactory
  );

  // Blocking call
  server.serve();

  return 0;
}

