/******************************************************************************
 *                      IRequestHandler.hpp
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
 * @(#)$RCSfile: IRequestHandler.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/04 08:54:26 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IREQUESTHANDLER_HPP 
#define CASTOR_IREQUESTHANDLER_HPP 1

// Include Files
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class IAddress;

  /**
   * Request handler abstract interface
   */
  class IRequestHandler {

  public:

    /**
     * virtual destructor
     */
    virtual ~IRequestHandler(){};

    /**
     * returns an address to the next request to handle in
     * the database.
     * Note that the caller is responsible for the deallocation
     * of the address. Note also that the database transaction
     * is not commited after this call. The caller is responsible
     * for the commit once it made sure that no request can be
     * forgotten whatever happens.
     * @exception Exception throws an Exception in case of error
     */
    virtual IAddress* nextRequestAddress()
      throw (castor::exception::Exception) = 0;

    /**
     * returns the number of requests handle in the database.
     * @exception Exception throws an Exception in case of error
     */
    virtual unsigned int nbRequestsToProcess()
      throw (castor::exception::Exception) = 0;
    
  };

} // end of namespace castor

#endif // CASTOR_IREQUESTHANDLER_HPP
