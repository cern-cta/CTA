#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2025 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

process_name="maintd"
log_dir="/var/log/cta/"
TIMEOUT_SEC=10

if [ "$(find ${log_dir} -type f | wc -l)" -ne 1 ]; then
  echo "ERROR: More than one log file found at ${log_dir}."
  exit 1
fi

# This is a rather simple and stupid test
# Because we are using configmaps, we can't actually change the config file
# Instead, we just send a SIGHUP and wait for a "Reload config" message
# Not very robust, but for now it will do
log_file=$(find ${log_dir} -type f)
echo "Found log file: ${log_file}"

# Count SIGHUP occurrences before
log_message_to_check="Reloading config"
num_messages_before=$(grep -c "$log_message_to_check" "$log_file" || true)
echo "Before sending SIGHUP: ${num_messages_before} \"$log_message_to_check\" log entries found"

echo "Sending SIGHUP to process"
/bin/pkill -SIGHUP -u cta "$process_name" 2> /dev/null || true

# Wait until a new log message arrives
deadline=$((SECONDS + TIMEOUT_SEC))
echo "Waiting for new \"$log_message_to_check\" to appear in the logs"
while [ $SECONDS -lt $deadline ]; do
    num_messages_now=$(grep -c "$log_message_to_check" "$log_file" || true)
    if [ "$num_messages_now" -eq $((num_messages_before + 1)) ]; then
        echo "Found new \"$log_message_to_check\" message"
        break
    fi
    sleep 1
done

num_messages_after=$(grep -c "$log_message_to_check" "$log_file" || true)
echo "After sending SIGHUP: ${num_messages_after} \"$log_message_to_check\" log entries found"

if [ "$num_messages_after" -ne $((num_messages_before + 1)) ]; then
    echo "ERROR: Expected exactly one new \"$log_message_to_check\", but count is $num_messages_after (before was $num_messages_before)."
    exit 1
fi

echo "Test successful"
echo "Done!"
