#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This script can be sourced in any other script and will prepend ALL output of stdout and stderror with the corresponding log prefix
# This will only happen if the BASH_LOGGING_ENABLED environment variable is explicitly set to true

readonly _log_bold=$'\e[1m'
readonly _log_reset=$'\e[0m'

readonly _log_red=$'\e[1m\e[31m'
readonly _log_green=$'\e[1m\e[32m'
readonly _log_yellow=$'\e[1m\e[33m'
readonly _log_blue=$'\e[1m\e[34m'
readonly _log_magenta=$'\e[1m\e[35m'
readonly _log_cyan=$'\e[1m\e[36m'


# By default trap overwrites whatever existing traps there are
# This means this breaks traps in other scripts
add_trap() {
  local new_cmd="$1"
  local sig="$2"

  # Get existing trap command (if any)
  local existing
  existing=$(trap -p "$sig" | awk -F"'" '{print $2}')

  if [[ -z "$existing" ]]; then
    trap "$new_cmd" "$sig"
  else
    trap "$existing; $new_cmd" "$sig"
  fi
}

# This function is invoked on fatal failures.
die() {
  echo "$@" >&2
  exit 1
}

# This function displays some error and exits with failure.
die_usage() {
  echo "$@" >&2
  usage
  exit 1
}

# Like die_usage() but with "Error: " prefix in the message.
error_usage() {
  die_usage "Error: $@"
}

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

  add_trap __log_end EXIT

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

# The following are meant to be used directly

print_header() {
  local term_width=${COLUMNS:-$(tput cols)}  # Get terminal width (default to tput)
  local msg="$(basename "$0"): $1"
  local border_char="="
  local separator=$(printf "%-${term_width}s" | tr ' ' "${border_char}")
  # Calculate padding for centering
  local msg_length=${#msg}
  local padding=$(( (term_width - msg_length) / 2 ))
  [[ $padding -lt 0 ]] && padding=0  # Avoid negative padding for small terminals
  echo
  echo "${_log_bold}${separator}${_log_reset}"
  echo "${_log_bold}$(printf "%*s%s" "$padding" "" "$msg")${_log_reset}"
  echo "${_log_bold}${separator}${_log_reset}"
  echo
}

log_task() {
  echo "==> $*"
}

log_success() {
  printf "%s%s%s\n" "${_log_bold}${$_log_green}" "$*" "$_log_reset"
}

log_warn() {
  printf "%s%s%s\n" "${_log_bold}${$_log_yellow}" "$*" "$_log_reset"
}

log_error() {
  printf "%s%s%s\n" "${_log_bold}${$_log_red}" "$*" "$_log_reset"
}
