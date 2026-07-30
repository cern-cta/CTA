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
  log_error "$@"
  exit 1
}

# This function displays some error and exits with failure.
die_usage() {
  log_error "$@"
  usage
  exit 1
}

# Like die_usage() but with "Error: " prefix in the message.
error_usage() {
  die_usage "Error: $@"
}

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
  printf "%s%s%s\n" "$_log_green" "$*" "$_log_reset"
}

log_warn() {
  printf "%s%s%s\n" "$_log_yellow" "$*" "$_log_reset"
}

log_error() {
  printf "%s%s%s\n" "$_log_red" "$*" "$_log_reset" >&2
}

log_run() {
  log_task "Running: $*"
  "$@"
}
