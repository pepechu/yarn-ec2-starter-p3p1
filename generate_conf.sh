#!/usr/bin/env bash

YARN_EC2_SDK_DIR="$(cd `dirname "$0"` && pwd -P)"

export YARN_EC2_SDK_DIR

"${YARN_EC2_SDK_DIR}/bin/generate_conf" "$@"
