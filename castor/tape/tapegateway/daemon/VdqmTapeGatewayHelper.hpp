/******************************************************************************
 *                      VdqmTapeGatewayHelper.hpp
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
 * helper class for accessing VDQM from C++
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP 

// Include Files
#include "vdqm.h"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace tape {
    
    namespace tapegateway {

      /**
       * helper class for accessing VDQM from C++
       */
      class VdqmTapeGatewayHelper {

        public:

        /**
         * connect to VDQM
         * @exception throws CASTOR exception if not successful
         */
	void connectToVdqm() throw(castor::exception::Exception);

        /**
         * create a request in VDQM
         * @param vid the tape concerned
         * @param dgn the concerned dgn
         * @param mode the access mode of the tape (WRITE_DISABLE or WRITE_ENABLE)
         * @param port the port on which VDQM should be contacted
         * @param priority the priority to be used for this request
         * @exception throws CASTOR exception if not successful
         */
	int createRequestForAggregator(const std::string &vid,
                                       const char *dgn,
                                       const int mode,
                                       const int port,
                                       const int priority)
          throw (castor::exception::Exception);

        /**
         * validate a VDQM request created with createRequestForAggregator
         * @exception throws CASTOR exception if not successful
         */
	void confirmRequestToVdqm() throw (castor::exception::Exception);

        /**
         * check existence of a given request in VDQM
         * @param mountTransactionId the id of the request to check
         * @exception throws CASTOR exception if not successful
         */
	static void checkVdqmForRequest(const int mountTransactionId)
          throw (castor::exception::Exception);

        /**
         * disconnect to VDQM
         * @exception throws CASTOR exception if not successful
         */
	void disconnectFromVdqm()throw (castor::exception::Exception);

      public:

        /// the connection to VDQM, when connected
	vdqmnw_t* m_connection;

      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_VDQMTAPEGATEWAYHELPER_HPP 
