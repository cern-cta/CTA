#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Keep the publication token out of the remote URL and Git command arguments.
case "$1" in
  *Username*) printf '%s\n' 'oauth2' ;;
  *Password*) printf '%s\n' "${DOCS_PUBLISH_TOKEN:?DOCS_PUBLISH_TOKEN is required}" ;;
esac
