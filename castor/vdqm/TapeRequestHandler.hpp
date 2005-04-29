/******************************************************************************
 *                      TapeRequestHandler.hpp
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
 * @(#)RCSfile: TapeRequestHandler.hpp  Revision: 1.0  Release Date: Apr 19, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _TAPEREQUESTHANDLER_HPP_
#define _TAPEREQUESTHANDLER_HPP_

#include "castor/exception/Exception.hpp"
#include "AbstractRequestHandler.hpp"

typedef struct vdqmHdr vdqmHdr_t;
typedef struct vdqmVolReq vdqmVolReq_t;

namespace castor {

  namespace vdqm {

    /**
     * The TapeRequestHandler provides functions to handle all vdqm related
     * tape request issues.
     */
    class TapeRequestHandler : public AbstractRequestHandler {

    public:

      /**
       * Constructor
       */
			TapeRequestHandler();
			
			/**
			 * This function replaces the old vdqm_NewVolReq C-function. It stores
			 * the request into the data Base
			 */
			void newTapeRequest(vdqmHdr_t *hdr, vdqmVolReq_t *VolReq) 
				throw (castor::exception::Exception);
       
       
    }; // class TapeRequestHandler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEREQUESTHANDLER_HPP_
