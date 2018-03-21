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
#include <fstream>
#include <sstream>
#include <string>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include "PhaseOnePolicyServerHandler.h"
#include "Assignment.h"
#include "Job.h"
#include "Policy.h"
#include "Utility.h"

/**
 * @brief The default value for the max machines used by the handler.
 */
#define MAX_MACHINES 256

/**
 * @brief The simulator creates an instance of the PolicyServerHandler
 * and calls the Schedule function on it using the set of jobs and machies
 * available and logs the results to the outputfile.
 */
template <int maxPossibleMachines>
class PolicySimulator {

private:
  /**
   * @brief Loads a JSON document.
   *
   * @param jsonf The path to the JSON file.
   * @return A JSON document.
   */
  static rapidjson::Document* ParseInputFile(const char *inputf) {
    std::ifstream ifs(inputf);
    std::stringstream buf;
    buf << ifs.rdbuf();
    std::string input = buf.str();
    rapidjson::Document* d = new rapidjson::Document;
    d->Parse<rapidjson::kParseCommentsFlag>(input.c_str());
    return d;
  }

  /**
   * @brief Gets the total number of machines.
   *
   * @param testCase The test case
   * @return The rack capacities
   */
  static std::vector<int> GetRackCapacities(const rapidjson::Value& testCase) {
    std::vector<int> rackCapacities;

    // Count the number of machines in each rack.
    const rapidjson::Value& rack_cap = testCase["rack_cap"];
    for (size_t i = 0; i < rack_cap.Size(); ++i) {
      rackCapacities.push_back(rack_cap[i].GetInt());
    }

    return rackCapacities;
  }

  /**
   * @brief Gets the number of large machine racks.
   *
   * @param testCase The test case
   * @return The number of large machine racks
   */
  static int GetNumLargeMachineRacks(const rapidjson::Value& testCase) {
    // Count the number of machines in each rack.
    const rapidjson::Value& numLargeMachineRacks = testCase["numLargeMachineRacks"];
    return numLargeMachineRacks.GetInt();
  }

  /*
   * @brief Gets the policy to be simulated.
   *
   * @param d The JSON document.
   * @return The policy type to be simulated.
   */
  static const policy_t::type GetPolicy(const std::string& policyStr) {
    fprintf(stderr, "Running with policy %s\n", policyStr.c_str());
    if (policyStr.compare("fifo-r") == 0) {
      return policy_t::FIFO_RANDOM;
    } else if (policyStr.compare("fifo-h") == 0) {
      return policy_t::FIFO_HETER;
    } else if (policyStr.compare("sjf-h") == 0) {
      return policy_t::SJF_HETER;
    } else {
      fprintf(stderr, "Invalid policy string %s\n", policyStr.c_str());
      fprintf(stderr, "Exit...\n");
      exit(1);
    }
  }

  /**
  * @brief Gets the JobQueue.
  *
  * @param testQueue The JSON object.
  * @return The deque object of a set of Jobs.
  */
  static std::shared_ptr<std::deque<Job>> GetJobQueue(const rapidjson::Value& testQueue) {
    std::shared_ptr<std::deque<Job>> jobQueue(new std::deque<Job>());
    const std::string mpiString ("MPI");

    for (const auto& jobJson : testQueue.GetArray()) {
      Job job;
      job.jobId = jobJson["jobID"].GetInt();
      std::string jobType = jobJson["jobType"].GetString();

      // Job is always either MPI or GPU
      if (jobType.compare(mpiString) != 0) {
        job.jobType = Job::JOB_GPU;
      } else {
        job.jobType = Job::JOB_MPI;
      }

      job.k = jobJson["k"].GetUint();
      job.duration = jobJson["duration"].GetDouble();
      job.slowDuration = jobJson["slowDuration"].GetDouble();

      jobQueue->push_back(job);
    }

    return jobQueue;
  }

  /**
   * @brief Writes a JSON to the output file.
   *
   * @param outputf The path to the output file.
   * @param d A JSON document.
   */
  static void WriteOutputFile(const char *outputf, rapidjson::Document *d) {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    d->Accept(writer);

    std::ofstream ofs(outputf);
    ofs << buffer.GetString();
    fprintf(stderr, "Wrote simulation results to %s\n", outputf);
  }

public:

  /**
  * @brief Reads in the job queue and machines from the input file and
  * calls the Schedule function on the handler. The results are added
  * to a JSON document which is later written out to the output file.
  *
  * @param inputf Filename of the input file.
  * @param outputf Filename of the output file.
  * @param policyStr The name of the policy
  */
  static void Simulate(const char *inputf, const char *outputf, const char *policyStr) {
    const policy_t::type policy = GetPolicy(std::string(policyStr));
    auto inputJson  = ParseInputFile(inputf);

    rapidjson::Document* outputJson = new rapidjson::Document;
    rapidjson::Document::AllocatorType& allocator =
        outputJson->GetAllocator();

    rapidjson::Value& outputArray = outputJson->SetArray();
    const rapidjson::Value &testCases = (*inputJson)["testCases"];
    int testCaseCount = 0;

    for (const auto& testCase : testCases.GetArray()) {
      const std::vector<int> rackCap = GetRackCapacities(testCase);
      int numMachines = Utility::GetNumMachines(rackCap);
      const int numLargeMachineRacks = GetNumLargeMachineRacks(testCase);

      testCaseCount++;
      const rapidjson::Value& testQueue = testCase["queue"];
      auto jobQueue = GetJobQueue(testQueue);

      std::bitset<maxPossibleMachines> machines;
      machines.flip();

      // Fill in the bitset here as the function can't be templatized
      const rapidjson::Value& machinesJson = testCase["machines"];
      for(size_t i = 0; i < machinesJson.Size(); i++) {
        machines[machinesJson[i].GetInt()] = 0;
      }

      PhaseOnePolicyServerHandler<maxPossibleMachines> handler(
        policy, rackCap, numLargeMachineRacks, numMachines
      );

      const auto assignment = handler.Schedule(*(jobQueue.get()), machines);

      // Construct output object
      rapidjson::Value outputEntry(rapidjson::kObjectType);
      rapidjson::Value testCaseObject(testCase, allocator);

      outputEntry.AddMember("input", testCaseObject, allocator);

      // Add assignment if present
      rapidjson::Value assignmentJson(rapidjson::kObjectType);

      if (assignment != nullptr) {
        Job scheduledJob = assignment->job();
        assignmentJson.AddMember("jobID", scheduledJob.jobId, allocator);

        std::set<int32_t> selectedMachines = assignment->machines();
        rapidjson::Value selectedMachinesJson(rapidjson::kArrayType);
        for (const auto& machine : selectedMachines) {
          selectedMachinesJson.PushBack(machine, allocator);
        }

        assignmentJson.AddMember("machines", selectedMachinesJson, allocator);
      }

      outputEntry.AddMember("output", assignmentJson, allocator);
      outputArray.PushBack(outputEntry, allocator);
    }

    fprintf(stderr, "Simulated %d test cases!\n", testCaseCount);
    WriteOutputFile(outputf, outputJson);

    delete inputJson;
    delete outputJson;
  }

};

/**
 * @brief Main function which takes the inputfile, outputfile and policy.
 * It invokes the simulator.
 * @params argv[1] File containing the input trace.
 * @params argv[2] File to log the output of the trace.
 *
 */
int main(int argc, char **argv) {
  if (argc == 4) {
    PolicySimulator<MAX_MACHINES>::Simulate(argv[1], argv[2], argv[3]);
  } else {
    fprintf(stdout, "USAGE: %s <inputfile> <outputfile> <policy>", argv[0]);
  }

  return 0;
}
