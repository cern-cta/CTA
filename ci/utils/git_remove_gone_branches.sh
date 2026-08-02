#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

git fetch -p

mapfile -t gone_branches < <(
  git branch -vv | awk '/: gone]/{if ($1 == "*") print $2; else print $1}'
)

if [[ ${#gone_branches[@]} -eq 0 ]]; then
  echo "No local branches with a gone remote were found."
  exit 0
fi

echo "The following local branches no longer exist on the remote:"
printf '  %s\n' "${gone_branches[@]}"
read -r -p "Delete these branches? [y/N] " confirmation

if [[ "$confirmation" != "y" && "$confirmation" != "yes" ]]; then
  echo "No branches were deleted."
  exit 0
fi

git branch -D -- "${gone_branches[@]}"
