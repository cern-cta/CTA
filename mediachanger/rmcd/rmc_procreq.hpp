/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

struct rmc_srv_rqst_context {
  const char* localhost;
  int rpfd;
  char* req_data;
  const char* clienthost;
};

int rmc_srv_export(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_findcart(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_getgeom(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_import(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_mount(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_readelem(const struct rmc_srv_rqst_context* const rqst_context);
int rmc_srv_unmount(const struct rmc_srv_rqst_context* const rqst_context);
