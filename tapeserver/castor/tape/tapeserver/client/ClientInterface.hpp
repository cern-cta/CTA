/****************************************************************************** 
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
/* 
 * Author: dcome
 *
 * Created on March 18, 2014, 12:27 PM
 */

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/server/Threading.hpp"
#include "scheduler/MountType.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  

class ClientInterface {

public :
  /**
   * Class holding the timing information for the request/reply,
   * and the message sequence Id.
   */
  class RequestReport {
  public:
    RequestReport(): transactionId(""),
            connectDuration(0), sendRecvDuration(0) {}
    std::string transactionId;
    double connectDuration;
    double sendRecvDuration;
  };
  
  /**
   * Class holding the result of a Volume request
   */
    class VolumeInfo {
    public:
      VolumeInfo() {};
      /** The VID we will work on */
      std::string vid;
      /** The density of the volume */
      std::string density;
      /** The label field seems to be in disuse */
      std::string labelObsolete;
      /** The mount type: archive or retrieve */
      cta::MountType::Enum mountType;
    };
    
      /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     * @param transactionReport Placeholder to network timing information,
     * populated during the call and used by the caller to log performance 
     * and context information
     * @param errorMsg (sent to the client)
     * @param errorCode (sent to the client)
     */
    virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
    RequestReport &transactionReport)  = 0;
    
    
    /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     */
    virtual void reportEndOfSession(RequestReport &report) = 0;
    
    
    virtual ~ClientInterface(){}
};

}}}}

