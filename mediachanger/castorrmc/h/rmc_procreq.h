/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

struct rmc_srv_rqst_context {
  const char *localhost;
  int rpfd;
  char *req_data;
  const char *clienthost;
};

int rmc_srv_export  (const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_findcart(const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_getgeom (const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_import  (const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_mount   (const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_readelem(const struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_unmount (const struct rmc_srv_rqst_context *const rqst_context);
