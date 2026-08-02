#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

source "$(dirname "${BASH_SOURCE[0]}")/log_utils.sh"

usage() {
  echo "Usage: $0 [--all]"
  echo "Lists remote branches whose tip commit was written by the current Git user."
}

show_all=false

case "${1:-}" in
  "") ;;
  --all) show_all=true ;;
  -h|--help)
    usage
    exit 0 ;;
  *)
    log_error "Unsupported argument: $1"
    usage
    exit 1 ;;
esac

if [[ $# -gt 1 ]]; then
  log_error "Too many arguments."
  usage
  exit 1
fi

if $show_all; then
  git for-each-ref \
    --format='%(authoremail:trim) %09 %(authorname) %09 %(refname)' \
    --sort=refname \
    --sort=authorname \
    --sort=authoremail \
    refs/remotes
  exit 0
fi

author_email=$(git config user.email || true)
if [[ -z "$author_email" ]]; then
  log_error "Git user.email is not configured."
  exit 1
fi

log_warn "Showing branches for ${author_email}. Run with --all to see branches for all authors."

git for-each-ref \
  --format='%(authoremail:trim)%09%(authorname)%09%(refname)' \
  --sort=refname \
  --sort=authorname \
  refs/remotes | awk -F '\t' -v email="$author_email" '$1 == email {
    printf " %s \t %s \t %s\n", $1, $2, $3
  }'
