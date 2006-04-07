/******************************************************************************
 *                      castor/stager/RequestHelper.cpp
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
 * @(#)$RCSfile: RequestHelper.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2006/04/07 14:50:11 $ $Author: gtaur $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/RequestHelper.hpp"
#include "stager_client_api_common.h"
#include <iostream>
#include <string>

#define DEFAULT_HOST "stagepublic"
#define DEFAULT_PORT 5015
#define DEFAULT_SVCCLASS ""
#define DEFAULT_VERSION 1

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::RequestHelper::RequestHelper(castor::stager::Request* req) throw() :
  m_request(req) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::RequestHelper::~RequestHelper() throw() {};

//------------------------------------------------------------------------------
// setOptions
//------------------------------------------------------------------------------
void castor::stager::RequestHelper::setOptions(struct stage_options* opts) {
  // Setting the options
     if(opts !=0){
	if ( opts->stage_host == NULL || strcmp(opts->stage_host,"")) {
		if (opts->stage_host){free(opts->stage_host);}
		opts->stage_host=strdup(DEFAULT_HOST);
	}
	if ( opts->service_class == NULL || strcmp(opts->service_class,"")) {
		if (opts->service_class){free(opts->service_class);}
		opts->service_class=strdup(DEFAULT_SVCCLASS);
	}
	if (opts->stage_version<=0){opts->stage_version=DEFAULT_VERSION;} 
	if (opts->stage_port<=0){opts->stage_port=DEFAULT_PORT;} 
        }
}



