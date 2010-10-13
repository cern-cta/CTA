/******************************************************************************
 *                      stager_client_api.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: stager_client_api.h,v $ $Revision: 1.48 $ $Release$ $Date: 2009/05/13 08:37:56 $ $Author: sponcec3 $
 *
 * the internal extensions to the client API to the castor stager
 *
 * @author castor dev team
 *****************************************************************************/

#include "stager_client_api.h"
#include "castor/query/DiskPoolQueryType.hpp"

/**
 * stage_diskpoolquery_internal
 * Returns summary information about a CASTOR diskpool
 * 
 * @param diskPoolName name of the diskPool to query
 * @param response the diskPool description
 * @param opts CASTOR stager specific options
 * @param reportAvailableSpace whether to report only available space or not
 *
 * @returns 0 in case of success, -1 otherwise
 * @note the subparts of response are allocated by the call
 *       and therefore should be freed by the client.
 */
int stage_diskpoolquery_internal (char *diskPoolName,
                                  struct stage_diskpoolquery_resp *response,
                                  struct stage_options* opts,
                                  enum castor::query::DiskPoolQueryType queryType);

/**
 * stage_diskpoolsquery
 * Returns summary information about CASTOR diskpools
 * 
 * @param responses List of diskPool descriptions
 * @param nbresps number of diskPool descriptions in the list
 * @param opts CASTOR stager specific options
  * @param reportAvailableSpace whether to report only available space or not
*
 * @returns 0 in case of success, -1 otherwise
 * @note responses is allocated by the call, as well as all its subparts
 *       and therefore should be freed by the client.
 */
int stage_diskpoolsquery_internal (struct stage_diskpoolquery_resp **responses,
                                   int *nbresps,
                                   struct stage_options* opts,
                                   enum castor::query::DiskPoolQueryType queryType);
