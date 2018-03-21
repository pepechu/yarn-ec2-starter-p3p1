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

POLICY=""
POLSRVR="MyPolicyServer"

# Prints the usage of the script
usage() {
    ERR_STR="Usage: $0\n"
    ERR_STR+="[-p <string>\t--policy=<string>\tName of the policy, options: {fifo-r, fifo-h, sjf-h, custom}]\n"
    ERR_STR+="[-s<string>\t--server=<string>\tName of the server, defaults to $POLSRVR]"
    echo -e $ERR_STR 1>&2;
    exit 1;
}

# Call getopt, if unsuccessfully parse input, then exit
TEMP=`getopt -o p:s:: -l policy:,server: -n 'run_policy_server.sh' -- "$@"`
if [ $? -ne 0 ]; then
    usage
fi

eval set -- "$TEMP"

# extract options and their arguments into variables.
while true ; do
    case "$1" in
        -p|--policy)
            # Sets the policy to use.
            case "$2" in
                "") shift 2 ;;
                *) POLICY=$2 ; shift 2 ;;
            esac ;;
        -s|--server)
            # This is an optional variable to set the server name.
            case "$2" in
                "") POLSRVR=$POLSRVR ; shift 2 ;;
                *) POLSRVR=$2 ; shift 2 ;;
            esac ;;
        --) shift ; break ;;
        *) usage;;
    esac
done

# If either policy or server name are undefined, then exit.
if [ -z "$POLSRVR" ] || [ -z "$POLICY" ]; then
    usage
fi

# Make sure that policy is one of the following
case $POLICY in
    fifo-r|fifo-h|sjf-h|custom) ;;
    *) usage;;
esac

# Check that policy server is built
if [ ! -f src/$POLSRVR ] ; then
    echo "FATAL: $POLSRVR binary not built... exit"
    exit 1
fi

# Remove the old job-conf.json
rm -f ${YARN_EC2_SDK_DIR}/job-conf.json

# Generate the new job-conf.json
"${YARN_EC2_SDK_DIR}/generate_conf.sh" --policy $POLICY

# Verify that job-conf.json exists
if [ ! -f ${YARN_EC2_SDK_DIR}/job-conf.json ] ; then
    echo "job-conf.json does not exist"
    exit 1
fi

# Stop YARN....
sudo ydown
sudo hddown

# Kill any currently running policy servers....
sudo killall $POLSRVR
sudo killall java

# Delete the RM files
sudo rm -f /srv/yarn/logs/yarn-root-resourcemanager-*.log

# Purge old log files....
sudo rm -f /tmp/my_policy_server.log
sudo rm -f /tmp/my_policy_server.out

sudo rm -f /srv/yarn/results.txt

# Restart YARN....
sudo hdup
sleep 0.1

sudo yup
echo ""
echo "Sleeping for a minute..."
sleep 1m

# Start a new policy server daemon with job-conf.json
echo "Starting policy server..."
export MY_CONFIG="$(cd ${YARN_EC2_SDK_DIR} && pwd -P)/job-conf.json"
nohup src/$POLSRVR </dev/null 1>/tmp/my_policy_server.out 2>/tmp/my_policy_server.log &
echo "Redirecting stdout to /tmp/my_policy_server.out..."
echo "Redirecting stderr to /tmp/my_policy_server.log..."
sleep 0.1

echo ""
echo "OK - running in background"
echo "--------------------"
echo "!!! POLSRVR UP !!!"

exit 0
