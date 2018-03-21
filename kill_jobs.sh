#!/bin/bash -u

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

YARN_EC2_SDK_DIR="$(cd `dirname "$0"` && pwd -P)"

ping ibuki -c 1 &>/dev/null

if [ $? -ne 0 ] ; then
    echo "NOTE: `basename $0` is supposed to be invoked at an ec2 instance... exit"
    exit 1
fi

if [ `id -u` -ne 0 ] ; then
    echo "NOTE: `basename $0` must be executed as root... exit"
    exit 1
fi

HADOOP_PREFIX=/srv/yarn
export HADOOP_PREFIX

HADOOP_CONF_DIR=$HADOOP_PREFIX/conf
export HADOOP_CONF_DIR

HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME

HADOOP_YARN_HOME=$HADOOP_PREFIX
export HADOOP_YARN_HOME

YARN_HOME=$HADOOP_PREFIX
export YARN_HOME

YARN_LOG_DIR=$HADOOP_PREFIX/logs
export YARN_LOG_DIR

YARN_PID_DIR=/tmp
export YARN_PID_DIR

"${YARN_EC2_SDK_DIR}/bin/kill_jobs" "$@"

exit 0
