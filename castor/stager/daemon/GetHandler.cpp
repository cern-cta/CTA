/******************************************************************************
 *                      GetHandler.cpp
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
 * Implementation of the get subrequest's handler
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/GetHandler.hpp"

namespace castor{
  namespace stager{
    namespace daemon{

      /**
       * constructor
       */
      GetHandler::GetHandler(RequestHelper* reqHelper)
        throw(castor::exception::Exception) :
        OpenRequestHandler(reqHelper), m_handleGetStatement(0) {
      };

      /**
       * handler for the get requests
       */
      bool GetHandler::handle() throw (castor::exception::Exception) {
        // call parent's handler method
        OpenRequestHandler::handle();
        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();
        // call PL/SQL handleGet method
        reqHelper->stagerService->handleGet(reqHelper->castorFile->id(),
                                            reqHelper->subrequest->id(),
                                            reqHelper->cnsFileid,
                                            reqHelper->cnsFilestat.filesize);
        return true;
      }

    } //end namespace daemon
  } //end namespace stager
} //end namespace castor
