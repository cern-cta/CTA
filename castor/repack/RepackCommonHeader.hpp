/******************************************************************************
 *                      RepackCommoonHeader.hpp
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
 * This is the common header for the Repack system. 
 *
 * @author Felix Ehm
 *****************************************************************************/


#ifndef REPACKCOMMONHEADER
#define REPACKCOMMONHEADER

#include <iostream>
#include <string>
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "h/stager_client_api_common.hpp"   // for stage_trace("..")
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "RepackRequest.hpp"
#include "RepackSubRequest.hpp"
#include "RepackResponse.hpp"
#include "RepackSegment.hpp"
#include "RepackAck.hpp"
#include "RepackServer.hpp"

namespace castor{
  namespace repack {

    enum RepackConstants {

    CSP_REPACKSERVER_PORT = 62800,  // the standard server port
    CSP_REPACKPOLLTIME = 240,       // the standard polling time for cleaner and monitor

    DEFAULT_STAGER_PORT = 9002,

    };

   
    }
}


#endif //REPACKCOMMONHEADER
