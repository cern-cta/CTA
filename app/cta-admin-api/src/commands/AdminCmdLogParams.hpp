/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

#include "cta_frontend.pb.h"

namespace cta::admin_api::commands {

/// @brief Add the command and subcommand names, and the options passed on the command line, to a log param container
///
/// String list options are logged as their number of items, as the lists can be long.
///
/// @param[in,out] params    Container to add the parameters to
/// @param[in]     adminCmd  Admin command to log
void addAdminCmdLogParams(log::ScopedParamContainer& params, const admin::AdminCmd& adminCmd);

}  // namespace cta::admin_api::commands
