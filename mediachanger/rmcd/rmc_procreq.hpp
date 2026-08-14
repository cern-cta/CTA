/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

struct rmc_srv_rqst_context {
  const char* localhost;
  int rpfd;
  char* req_data;
  const char* clienthost;
};

//! Export/eject a cartridge from the robot
int rmc_srv_export(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Find cartridge(s)
int rmc_srv_findcart(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Get the robot geometry
int rmc_srv_getgeom(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Import/inject a cartridge into the robot
int rmc_srv_import(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Mount a cartridge into a drive
int rmc_srv_mount(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Read element status
int rmc_srv_readelem(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);

//! Dismount a cartridge from a drive
int rmc_srv_unmount(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context);
