#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


# This script can be sourced in any other script and will prepend ALL output of stdout and stderror with the corresponding log prefix
# This will only happen if the BASH_LOGGING_ENABLED environment variable is explicitly set to true


# Note that these functions are not meant to be used directly outside of this script; just use echo (and redirect to stderr if necessary)
# The reason is twofold: first the existing file descriptor redirection will cause it two prepend twice.
# Second, it hurts the portability of the script that sources this, and does not make it as easy to turn off logging.
__log_info() {
  local message="$*"
  echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO]  [$current_script]  $message"

}

__log_error() {
  local message="$*"
  echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] [$current_script]  $message" >&2
}

__log_start() {
  # Preserve original stdout and stderr
  exec 3>&1 4>&2

  # Set up pipes for stdout and stderr
  # The output of stdout will be redirected to __log_info
  # The output of stderr will be redirect to __log_error
  mkfifo /tmp/stdout_pipe_$$
  mkfifo /tmp/stderr_pipe_$$
  cat < /tmp/stdout_pipe_$$ | while IFS= read -r line; do __log_info "$line"; done &
  pid_stdout=$!
  cat < /tmp/stderr_pipe_$$ | while IFS= read -r line; do __log_error "$line"; done &
  pid_stderr=$!
  # Redirect stdout and stderr to the pipes
  exec 1> /tmp/stdout_pipe_$$ 2> /tmp/stderr_pipe_$$

  trap __log_end EXIT

  start_time=$(date +%s%N)
  echo "Starting: $current_script $*"
}

__log_end() {
  # Restore stdout and stderr; this also ensures that they are flushed
  exec 1>&3 3>&-
  exec 2>&4 4>&-

  # Close the pipes and wait for them to finish logging all the remaining lines
  rm /tmp/stdout_pipe_$$ /tmp/stderr_pipe_$$
  wait $pid_stdout
  wait $pid_stderr

  end_time=$(date +%s%N)
  elapsed_time=$(( end_time - start_time ))
  elapsed_time=$(printf "%019d" "$elapsed_time") # Pad to cover short durations
  __log_info "Elapsed time: $((10#${elapsed_time:0:-9})).${elapsed_time: -9:2} seconds"

}


if [[ $BASH_LOGGING_ENABLED -eq 1 ]]; then
  current_script=$(basename "$0")
  if [[ -z "$BASH_LOGGING_INITIALIZED" ]]; then
    # Don't reset stdout/stderr if this is the first script we initialize
    export BASH_LOGGING_INITIALIZED=1
  else
    # Reset stdout/stderr to ensure we can overwrite the current_script in the log output
    exec 1>&3 3>&-
    exec 2>&4 4>&-
  fi
  __log_start "$*"
fi

print_header() {
  local term_width=${COLUMNS:-$(tput cols)}  # Get terminal width (default to tput)
  local msg="$1"
  local border_char="="
  local separator=$(printf "%-${term_width}s" | tr ' ' "${border_char}")
  # Calculate padding for centering
  local msg_length=${#msg}
  local padding=$(( (term_width - msg_length) / 2 ))
  [[ $padding -lt 0 ]] && padding=0  # Avoid negative padding for small terminals
  echo
  echo "${separator}"
  printf "%*s%s\n" $padding "" "$msg"  # Print message centered
  echo "${separator}"
  echo
}
