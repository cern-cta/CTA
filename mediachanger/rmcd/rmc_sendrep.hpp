/*
 * SPDX-FileCopyrightText: 1998 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

int rmc_sendrep(cta::log::LogContext& lc, const int rpfd, const int rep_type, ...);
