/******************************************************************************
*                castor/stager/daemon/JobRequestSvcThread.hpp
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
* Service thread for handling Job oriented requests, i.e. Gets and Puts
*
* @author castor dev team
*****************************************************************************/

#pragma once

#include "castor/server/SelectProcessThread.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include <string>
#include "unistd.h"
#include "Cuuid.h"
#include "occi.h"

namespace castor {

  namespace stager {

    namespace daemon {
      
      /**
       * a small struct containing the details of a given request
       * and passed to the threads processing it
       */
      struct JobRequest : public castor::IObject {
        // to please virtual inheritance XXX We need something better !
        virtual ~JobRequest() throw() {};
        virtual void setId(u_signed64 id) {};
        virtual u_signed64 id() const { return 0; }
        virtual int type() const { return  0; }
        virtual IObject* clone() { return 0; }
        virtual void print(std::ostream& stream,
                           std::string indent,
                           castor::ObjectSet& alreadyPrinted) const {};
        virtual void print() const {};
        /// identifier of the subrequest
        u_signed64 srId;
        /// uuid of the request
        Cuuid_t requestUuid;
        /// type of the request
        int reqType;
        /// client identification
        uid_t euid;
        gid_t egid;
        /// name of the concerned file
        std::string fileName;
        /// name of the concerned svcClass
        std::string svcClassName;
        /// fileclass to be used
        u_signed64 fileClass;
        /// flags and modebits for file opening/creation
        int flags;
        int modebits;
        /// details on how to answer the client
        unsigned long clientIpAddress;
        unsigned short clientPort;
        int clientVersion;
      };

      class JobRequestSvcThread : public castor::server::SelectProcessThread {
        
      public: 

        /** constructor */
        JobRequestSvcThread() throw (castor::exception::Exception);

        /** virtual destructor */
        virtual ~JobRequestSvcThread() throw() {};
        
        /**
         * Select part of the service.
         * @return next operation to handle, 0 if none.
         */
        virtual castor::IObject* select() throw();

        /**
         * Process part of the service
         * @param param next operation to handle.
         */
        virtual void process(castor::IObject* subRequestToProcess) throw();

      private:

        /** helper function calling the PL/SQL method jobSubRequestToDo */
        JobRequest *jobSubRequestToDo() throw (castor::exception::Exception);

        /** helper function calling the PL/SQL method handleGetOrPut
         *  returns whether we should reply to client. Possible values are
         *    0 : do not answer
         *    1 : answer but this is not the last answer
         *    2 : answer and this is the last answer
         */
        int handleGetOrPut(const JobRequest *sr, const struct Cns_fileid &cnsFileid,
                           u_signed64 fileSize, u_signed64 stagerOpentimeInUsec)
        throw (castor::exception::Exception);
        
        /** helper function calling the PL/SQL method updateAndCheckSubRequest
         * returns whether we are the last subrequest of the corresponding resquest
         */
        bool updateAndCheckSubRequest(const u_signed64 srId, const int status)
        throw (castor::exception::Exception);

        /** helper function answering the client */
        void answerClient(const JobRequest *sr, const struct Cns_fileid &cnsFileid,
                          int status,  int errorCode, const std::string &errorMsg,
                          bool isLastAnswer = false)
        throw (castor::exception::Exception);

      };
      
    }// end namespace daemon
    
  } // end namespace stager
  
} //end namespace castor

