#!/usr/bin/env bash

YARN_EC2_SUBMIT_DIR="$(cd `dirname "$0"` && pwd -P)"

export YARN_EC2_SUBMIT_DIR

sudo apt-get install -y python-requests

"${YARN_EC2_SUBMIT_DIR}/bin/submit_p3p1" "$@"
