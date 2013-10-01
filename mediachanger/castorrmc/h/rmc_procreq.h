/******************************************************************************
 *                      rmc_procreq.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef _RMC_PROCREQ_H
#define _RMC_PROCREQ_H 1

struct rmc_srv_rqst_context {
  const char *localhost;
  int rpfd;
  char *req_data;
  const char *clienthost;
};

int rmc_srv_export(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_findcart(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_getgeom(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_import(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_mount(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_readelem(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_unmount(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_genericmount(struct rmc_srv_rqst_context *const rqst_context);
int rmc_srv_genericunmount(struct rmc_srv_rqst_context *const rqst_context);

#endif
