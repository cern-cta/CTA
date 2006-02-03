/******************************************************************************
 *                      stager_client_api_authid.hpp
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
 * @(#)$RCSfile: stager_client_api_authid.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/02/03 11:40:17 $ $Author: sponcec3 $
 *
 * Interface to set the Authorization id of a client
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_STAGER_CLIENT_API_AUTHID_HPP 
#define H_STAGER_CLIENT_API_AUTHID_HPP 1

namespace castor {

  namespace client {

    // forward declaration of BaseClient
    class BaseClient;

    /**
     * Sets the client authorization id of the given BaseClient
     * @param client The client concerned
     * @exception Exception throws an Exception in case of error
     */
    void DLL_DECL setClientAuthorizationId
    (castor::client::BaseClient &client)
      throw(castor::exception::Exception);

  }
}


#endif // H_STAGER_CLIENT_API_AUTHID_HPP
