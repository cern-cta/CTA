/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/AdminCmdOptions.hpp"
#include "commands/sync/SyncCmdContext.hpp"

#include "cta_admin.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::commands::sync {

/// @brief Handler of a non-streamed admin command, see handlers/Handlers.hpp
using SyncCmdHandler = void (*)(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief Finds the handler of an admin command
///
/// @return The handler of the <Cmd, SubCmd> pair of the command, or nullptr if it is not a (non-streamed) admin command
SyncCmdHandler findHandler(const admin::AdminCmd& adminCmd);

}  // namespace cta::admin_api::commands::sync
