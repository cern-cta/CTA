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
STRACE_SLEEP_SECS=2

if [ "$(find ${log_dir} -type f | wc -l)" -ne 1 ]; then
  echo "ERROR: More than one log file found at ${log_dir}."
  exit 1
fi

log_file=$(find ${log_dir} -type f)

# Install dependencies required for testing log rotation inside the tape server"
dnf -y install strace lsof


# Get PID of all taped processes
pid=$(pgrep "$process_name" -u cta)

if [ -z "${pid}" ]; then
    echo "ERROR: No $process_name process found."
    exit 1
fi

echo "Found '$process_name' process PID: ${pid}"

# Get number of file descriptor used to open the log file
log_file_fd=$(lsof -p "${pid}" | grep "${log_file}" | awk '{print $4}' | awk -F'[^0-9]' '{print $1}')
if [ -z "${log_file_fd}" ]; then
  echo "ERROR: No file descriptor found for log file ${log_file}."
  exit 1
else
  echo "Found file descriptor #${log_file_fd} being used to access ${log_file}."
fi

# Temporary files to store strace output
strace_tmp_file=$(mktemp)

# Run strace in the background for each taped process
echo "Launching 'strace' for process (background execution)..."
strace -e trace=close,open,openat -fp "${pid}" -o "${strace_tmp_file}" &
strace_pid=$!

# Wait for strace to start
sleep 1

# Send signal to trigger log rotation in all the taped processes
echo "Sending 'USR1' signal to '$process_name' process"
/bin/pkill -SIGUSR1 -u cta "$process_name" 2> /dev/null || true

# Sleep for ${STRACE_SLEEP_SECS} seconds to allow for strace to register events
echo "Waiting for ${STRACE_SLEEP_SECS} seconds..."
sleep ${STRACE_SLEEP_SECS}

# Finally, close strace processes
echo "Waiting is over. Stopping 'strace' processes."
kill "${strace_pid}"

# Wait for strace to stop
sleep 1

# Confirm that the file descriptor was reopened in all processes
echo "Checking '$process_name' file descriptor reopening..."
if [ "$(grep -c "close(${log_file_fd})" "${strace_tmp_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor #${log_file_fd} not closed in '$process_name'."
  exit 1
elif [ "$(grep -c "close(${log_file_fd})" "${strace_tmp_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor #${log_file_fd} closed more than once in '$process_name'."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} closed once in '$process_name'."
fi
if [ "$(grep "open" "${strace_tmp_file}" | grep -c "${log_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor for ${log_file} not reopened in '$process_name'."
  exit 1
elif [ "$(grep "open" "${strace_tmp_file}" | grep -c "${log_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor for ${log_file} reopened more than once in '$process_name'."
  exit 1
else
  echo "OK: File descriptor for ${log_file} reopened once in '$process_name'."
fi

# Finish test
echo "Cleaning up tmp files..."
rm -f "${strace_tmp_file}"

echo "Test successful"
echo "Done!"
