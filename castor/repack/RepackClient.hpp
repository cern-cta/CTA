/******************************************************************************
 *                      RepackClient.hpp
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
 * @(#)$RCSfile: RepackClient.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/01/23 14:56:44 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKCLIENT_HPP
#define REPACKCLIENT_HPP 1

/* Common includes */
#include "castor/repack/RepackCommonHeader.hpp"
#include <common.h>
#include <dlfcn.h>
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/repack/RepackServer.hpp"	/*TODO: remove! CSP_REPACKSERVERPORT  
											should be declared in Constants.hpp 
											*/
#include "Cgetopt.h"
#include "h/stager_client_api_common.h" 
#include "castor/repack/RepackRequest.hpp"
#include "castor/repack/RepackSubRequest.hpp"
#include "castor/repack/RepackRequest.hpp"

namespace castor {

 namespace repack {
  
  /**
   * CASTOR Repack main daemon.
   */
    
    /** 
      * struct to pass Parameters later on to the working thread
      */
    struct cmd_params{
      char* vid;
      char* pool;
    };

  class RepackClient : public castor::BaseObject{

  public:

    /**
     * constructor
     */
    RepackClient();

    /**
     * destructor
     */
    ~RepackClient()                                             throw();

    void RepackClient::run(int argc, char** argv);

  protected:
    bool parseInput(int argc, char** argv);
    castor::repack::RepackRequest* buildRequest()             throw ();
    void usage(std::string message)                     throw ();
    void setRemotePort() throw (castor::exception::Exception);
    void setRemoteHost() throw (castor::exception::Exception);

  private:
    int m_defaultport;
    std::string m_defaulthost;
    castor::ICnvSvc *svc;
    cmd_params cp;

  };

 } // end of namespace repack

} // end of namespace castor


#endif 

