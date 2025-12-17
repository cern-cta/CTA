/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::log {

const int EMERG = 0;    // system is unusable
const int ALERT = 1;    // action must be taken immediately
const int CRIT = 2;     // critical conditions
const int ERR = 3;      // error conditions
const int WARNING = 4;  // warning conditions
const int NOTICE = 5;   // normal but signification condition
const int INFO = 6;     // informational
const int DEBUG = 7;    // debug-level messages

/**
 * It is a convention of CTA to use syslog level of LOG_NOTICE to label
 * user errors.
 */
const int USERERR = NOTICE;

}  // namespace cta::log
