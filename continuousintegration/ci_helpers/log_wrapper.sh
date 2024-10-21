#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

# This script can be sourced in any other script and will prepend ALL output of stdout and stderror with the corresponding log prefix
# This will only happen if the BASH_LOGGING_ENABLED environment variable is explicitly set to true


# Note that these functions are not meant to be used directly outside of this script; just use echo (and redirect to stderr if necessary)
# The reason is twofold: first the existing file descriptor redirection will cause it two prepend twice.
# Second, it hurts the portability of the script that sources this, and does not make it as easy to turn off logging.
log_info() {
  local message="$*"
  echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] [$current_script] $message"
}

log_error() {
  local message="$*"
  echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] [$current_script] $message" >&2
}

log_start() {
  # Preserve original stdout and stderr
  exec 3>&1 4>&2

  # Set up pipes for stdout and stderr
  # The output of stdout will be redirected to log_info
  # The output of stderr will be redirect to log_error
  mkfifo /tmp/stdout_pipe_$$
  mkfifo /tmp/stderr_pipe_$$
  cat < /tmp/stdout_pipe_$$ | while read -r line; do log_info "$line"; done &
  pid_stdout=$!
  cat < /tmp/stderr_pipe_$$ | while read -r line; do log_error "$line"; done &
  pid_stderr=$!
  # Redirect stdout and stderr to the pipes
  exec 1> /tmp/stdout_pipe_$$ 2> /tmp/stderr_pipe_$$

  trap log_end EXIT
  start_time=$(date +%s)
  echo "Starting: $current_script $*"
}

log_end() {
  # Restore stdout and stderr; this also ensures that they are flushed
  exec 1>&3 3>&-
  exec 2>&4 4>&-

  # Close the pipes and wait for them to finish logging all the remaining lines
  rm /tmp/stdout_pipe_$$ /tmp/stderr_pipe_$$
  wait $pid_stdout
  wait $pid_stderr

  end_time=$(date +%s)
  elapsed_time=$(( end_time - start_time ))
  log_info "Elapsed time: ${elapsed_time} seconds"
}


if [[ $BASH_LOGGING_ENABLED -eq 1 ]]; then
  current_script=$(basename "$0")
  if [[ -z "$LOGGING_INITIALIZED" ]]; then
    export LOGGING_INITIALIZED=1
    log_start "$*"
  else
    # Reset stdout/stderr to ensure we can overwrite the current_script in the log output
    exec 1>&3 3>&-
    exec 2>&4 4>&-
    log_start "$*"
  fi
fi