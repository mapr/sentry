#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

USAGE="Usage: sentry-daemon.sh \
 (start <configuration file>|stop|status)"

PATH_BIN=$(dirname "${BASH_SOURCE-$0}")

SENTRY_PATH=${PATH_BIN}/..
PATH_LOG=${SENTRY_PATH}/logs
PATH_CONF=${SENTRY_PATH}/conf
WARDEN_SENTRY_CONF=/opt/mapr/conf/conf.d/warden.sentry.conf
DEFAULT_SENTRY_PORT=8038

find_sentry_port(){
if [ -f ${WARDEN_SENTRY_CONF} ]; then
    local SENTRY_PORT=$(sed -n "/service.port=/p" ${WARDEN_SENTRY_CONF})
    echo "$(echo "$SENTRY_PORT" | awk -F "=" '{print $2}')"
else
    echo "$DEFAULT_SENTRY_PORT"
fi
}

find_sentry_pid(){
local SENTRY_PORT=$(find_sentry_port)
local SENTRY_PID=$(lsof -i:"$SENTRY_PORT" | grep LISTEN | awk '{print $2}')
if [ -n "$SENTRY_PID" ]; then
    echo "$SENTRY_PID"
    else echo 0
fi
}

if [ $# -lt 1 ]; then
    echo "$USAGE"
    exit 1
fi

COMMAND=$1
CONF_FILE=$2

if [[ "$COMMAND" == 'start' && -z "$CONF_FILE" ]]; then
    echo 'Error: No configuration file is set!'
    echo "$USAGE"
    exit 1
fi


case ${COMMAND} in
 status)
     SENTRY_PROCESS_ID=$(find_sentry_pid)
     if [ "$SENTRY_PROCESS_ID" -gt 0 ];then
         echo "Sentry is running as process $SENTRY_PROCESS_ID."
         echo "$(date +%m.%d-%H:%M:%S) INFO : Process checked for sentry : TRUE" >> "$PATH_LOG/daemons.txt"
         exit 0;
     else
         echo "Sentry is not running."
         echo "$(date +%m.%d-%H:%M:%S) INFO : Process checked for sentry : FALSE" >> "$PATH_LOG/daemons.txt"
         exit 1;
     fi
 ;;
 stop)
     SENTRY_PROCESS_ID=$(find_sentry_pid)
     if [ "$SENTRY_PROCESS_ID" -gt 0 ];then
         kill -9 "$SENTRY_PROCESS_ID"
         exit 0
     else
         echo "Sentry is not running."
     fi
 ;;
 start)
     SENTRY_PROCESS_ID=$(find_sentry_pid)
     if [ "$SENTRY_PROCESS_ID" -gt 0 ];then
         echo "Sentry is already running with PID $SENTRY_PROCESS_ID. Stop it first."
     else
         echo "$(date +%m.%d-%H:%M:%S) INFO : Process starting for sentry " >> "$PATH_LOG/daemons.txt"
         nohup "${PATH_BIN}/sentry" --log4jConf "${PATH_CONF}/log4j.properties" --command service -c "${CONF_FILE}" > "$PATH_LOG/nohup.out" 2>&1 </dev/null &
         echo "$(date +%d-%H.%M.%S) INFO : Process started for sentry " >> "$PATH_LOG/daemons.txt"
     fi
esac