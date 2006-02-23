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
 * @(#)$RCSfile: RepackClient.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2006/02/23 12:38:01 $ $Author: felixehm $
 *
 * The Repack Client. This is the client part of the repack project, which just
 * sends and Request to the server. One Request can have serveral tapes 
 * or one tapepool to be repacked.
 * 
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKCLIENT_HPP
#define REPACKCLIENT_HPP 1

/* Common includes */
#include "castor/repack/RepackCommonHeader.hpp"
#include <common.h>
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/MessageAck.hpp"
#include "Cgetopt.h"
#include "h/stager_client_api_common.h" 

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
    /** 
     * parses the input and validates the parameters
     * @return bool 
     */
    bool parseInput(int argc, char** argv);
    /**
     * Builds the RepackRequest from the given parameters.
     * Either a pool name can be specified OR volume ids seperated by ':'
     * @return RepackRequest a new Repack Request or NULL
     */
    castor::repack::RepackRequest* buildRequest()             throw ();
    /**
     * Little method to show a short usage description.
     */
    void usage();
    /**
     * Little method to show the full help of the repack client.
     */
    void help();
    
    /**
     * Sets the remote port of the repack server. This port is used to contact
     * the server. The searching order for the port is the following:
     * 
     * 1.enviroment variable		: REPACK_PORT
     * 2.enviroment variable		: REPACK_PORT_ALT
     * 3./etc/castor/castor.conf	: REPACK PORT <portnumber>
     */
    void setRemotePort() throw (castor::exception::Exception);
    /**
     * Sets the hostname of the repack server. This host is used to contact
     * the server. The searching for the name order is the following:
     * 
     * 1.enviroment variable		: REPACK_HOST
     * 2.enviroment variable		: REPACK_HOST_ALT
     * 3./etc/castor/castor.conf	: REPACK HOST <hostname>
     */
    void setRemoteHost() throw (castor::exception::Exception);


  private:
    int m_defaultport;
   	/**
	 * Sets the Code for removal of given tape(s) from a repack que.
	 * The Tape(s) given as parameter after '-d' <VID1:VID2...>
	 */
	void deleteTapes();
	/**
	 * adds the given tape(s) in cp.vid (seperated by ':') to the repack request
	 * as new RepackSubRequest(s).
	 */
	int addTapes(RepackRequest *rreq);
    std::string m_defaulthost;
    castor::ICnvSvc *svc;
    cmd_params cp;

  };

 } // end of namespace repack

} // end of namespace castor


#endif 

