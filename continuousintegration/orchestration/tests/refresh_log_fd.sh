#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

log_dir="/var/log/cta/"
STRACE_SLEEP_SECS=2

if [ "$(find ${log_dir} -type f | wc -l)" -ne 1 ]; then
  echo "ERROR: More than one log file found at ${log_dir}."
  exit 1
fi

log_file=$(find ${log_dir} -type f)

# Install dependencies required for testing log rotation inside the tape server
echo "Installing missing RPMs in ${tape_server}... "
dnf -y install strace lsof

# Get PID of all taped processes
tpd_master_pid=$(pgrep "cta-tpd-master")
tpd_maint_pid=$(pgrep "cta-tpd-maint")
drive_name=$(ls /etc/cta | grep -E "cta-taped-.*\.conf" | xargs -I{} cat /etc/cta/{} | grep "DriveName" | awk '{print $NF}')
tpd_srv_pid=$(pgrep ${drive_name})

if [ -z "${tpd_master_pid}" ]; then
    echo "ERROR: No 'cta-tpd-master' process found."
    exit 1
fi
if [ -z "${tpd_maint_pid}" ]; then
    echo "ERROR: No 'cta-tpd-maint' process found."
    exit 1
fi
if [ -z "${tpd_srv_pid}" ]; then
    echo "ERROR: No 'cta-tpd-XXXXX' drive handler process found."
    exit 1
fi

echo "Found 'cta-tpd-master' process PID: ${tpd_master_pid}"
echo "Found 'cta-tpd-maint' process PID: ${tpd_maint_pid}"
echo "Found 'cta-tpd-XXXXX' drive handler process PID: ${tpd_srv_pid}"

# Get number of file descriptor used to open the log file
log_file_fd=$(lsof -p "${tpd_master_pid}" | grep "${log_file}" | awk '{print $4}' | awk -F'[^0-9]' '{print $1}')
if [ -z "${log_file_fd}" ]; then
  echo "ERROR: No file descriptor found for log file ${log_file}."
  exit 1
else
  echo "Found file descriptor #${log_file_fd} being used to access ${log_file}."
fi

# Temporary files to store strace output
tpd_master_tmp_file=$(mktemp)
tpd_maint_tmp_file=$(mktemp)
tpm_srv_tmp_file=$(mktemp)

# Run strace in the background for each taped process
echo "Launching 'strace' for all cta-taped processes (background execution)..."
strace -e trace=close,open,openat -fp "${tpd_master_pid}" -o "${tpd_master_tmp_file}" &
tpd_master_strace_pid=$!
strace -e trace=close,open,openat -fp "${tpd_maint_pid}" -o "${tpd_maint_tmp_file}" &
tpd_maint_strace_pid=$!
strace -e trace=close,open,openat -fp "${tpd_srv_pid}" -o "${tpm_srv_tmp_file}" &
tpd_srv_strace_pid=$!

# Wait for strace to start
sleep 1

# Send signal to trigger log rotation in all the taped processes
echo "Sending 'USR1' signal to 'cta-tpd-master' process"
kill -SIGUSR1 "${tpd_master_pid}"

# Sleep for ${STRACE_SLEEP_SECS} seconds to allow for strace to register events
echo "Waiting for ${STRACE_SLEEP_SECS} seconds..."
sleep ${STRACE_SLEEP_SECS}

# Finally, close strace processes
echo "Waiting is over. Stopping 'strace' processes."
kill "${tpd_master_strace_pid}"
kill "${tpd_maint_strace_pid}"
kill "${tpd_srv_strace_pid}"

# Wait for strace to stop
sleep 1

# Confirm that the file descriptor was reopened in all processes
echo "Checking 'cta-tpd-master' file descriptor reopening..."
if [ "$(grep -c "close(${log_file_fd})" "${tpd_master_tmp_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor #${log_file_fd} not closed in 'cta-tpd-master'."
  exit 1
elif [ "$(grep -c "close(${log_file_fd})" "${tpd_master_tmp_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor #${log_file_fd} closed more than once in 'cta-tpd-master'."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} closed once in 'cta-tpd-master'."
fi
if [ "$(grep "open" "${tpd_master_tmp_file}" | grep -c "${log_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor for ${log_file} not reopened in 'cta-tpd-master'."
  exit 1
elif [ "$(grep "open" "${tpd_master_tmp_file}" | grep -c "${log_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor for ${log_file} reopened more than once in 'cta-tpd-master'."
  exit 1
else
  echo "OK: File descriptor for ${log_file} reopened once in 'cta-tpd-master'."
fi

echo "Checking 'cta-tpd-maint' file descriptor reopening..."
if [ "$(grep -c "close(${log_file_fd})" "${tpd_maint_tmp_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor #${log_file_fd} not closed in 'cta-tpd-maint'."
  exit 1
elif [ "$(grep -c "close(${log_file_fd})" "${tpd_maint_tmp_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor #${log_file_fd} closed more than once in 'cta-tpd-maint'."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} closed once in 'cta-tpd-maint'."
fi
if [ "$(grep "open" "${tpd_maint_tmp_file}" | grep -c "${log_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor for ${log_file} not reopened in 'cta-tpd-maint'."
  exit 1
elif [ "$(grep "open" "${tpd_maint_tmp_file}" | grep -c "${log_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor for ${log_file} reopened more than once in 'cta-tpd-maint'."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} reopened once in 'cta-tpd-maint'."
fi

echo "Checking 'cta-tpd-XXXXX' drive handler file descriptor reopening..."
if [ "$(grep -c "close(${log_file_fd})" "${tpm_srv_tmp_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor #${log_file_fd} not closed in 'cta-tpd-XXXXX' drive handler ."
  exit 1
elif [ "$(grep -c "close(${log_file_fd})" "${tpm_srv_tmp_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor #${log_file_fd} closed more than once in 'cta-tpd-XXXXX' drive handler."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} closed once in 'cta-tpd-XXXXX' drive handler ."
fi
if [ "$(grep "open" "${tpm_srv_tmp_file}" | grep -c "${log_file}")" -eq 0 ]; then
  echo "ERROR: File descriptor for ${log_file} not reopened in 'cta-tpd-XXXXX' drive handler ."
  exit 1
elif [ "$(grep "open" "${tpm_srv_tmp_file}" | grep -c "${log_file}")" -gt 1 ]; then
  echo "ERROR: File descriptor for ${log_file} reopened more than once in 'cta-tpd-XXXXX' drive handler ."
  exit 1
else
  echo "OK: File descriptor #${log_file_fd} reopened once in 'cta-tpd-XXXXX' drive handler ."
fi

# Finish test
echo "Cleaning up tmp files..."
rm -f "${tpd_master_tmp_file}"
rm -f "${tpd_maint_tmp_file}"
rm -f "${tpm_srv_tmp_file}"

echo "Test successful"
echo "Done!"
