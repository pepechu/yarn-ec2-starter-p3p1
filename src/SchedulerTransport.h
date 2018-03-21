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

#include "Job.h"
#include "YARNTetrischedService.h"
#include "ClusterTracker.h"
#include "Assignment.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace ::alsched;

#pragma once

/**
 * @brief An entity that sends scheduling decisions to the TetriSched proxy scheduler.
 * All policy servers should make a call to the Scheduler transport upon scheduling decisions.
 */
template<int max_possible_machines>
class SchedulerTransport {

private:
  const std::string yarnHost_;
  const int yarnPort_;
  ClusterTracker<max_possible_machines> & clusterTracker_; // Notified of each scheduling decision

public:
  SchedulerTransport(const std::string yarnHost, const int yarnPort, ClusterTracker<max_possible_machines> & clusterTracker) :
    yarnHost_(yarnHost),
    yarnPort_(yarnPort),
    clusterTracker_(clusterTracker) {}

  /**
   * @brief This function communicates placement decisions to YARN
   * through the TetriSched proxy scheduler.
   *
   * @param assignment the assignment of job to its set of machines
   * @return true if the schedule was successful, false otherwise
   */
  bool Schedule(const std::shared_ptr<Assignment> & assignment) const {
    // Make sure that the assignment exists and is valid
    if (!clusterTracker_.Schedule(assignment)) {
      return false;
    }

    boost::shared_ptr<TTransport>
        socket(new TSocket(yarnHost_, yarnPort_));
    boost::shared_ptr<TTransport>
        transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol>
        protocol(new TBinaryProtocol(transport));
    YARNTetrischedServiceClient
        client(protocol);
    try {
      transport->open();
      client.AllocResources(assignment->job().jobId, assignment->machines());
      transport->close();
      fprintf(stderr,
        "JOB %d -> %d machines...\n",
        int(assignment->job().jobId),
        int(assignment->machines().size())
      );

      for (auto it = assignment->machines().begin(); it != assignment->machines().end(); ++it) {
        fprintf(stderr, " > machine %d\n", int(*it));
      }
    } catch (TException& tx) {
      fprintf(stderr, "ERROR calling YARNTetrischedService: %s\n", tx.what());
      fprintf(stderr, "Exit...\n");
      exit(1);
    }

    return true;
  }
};
