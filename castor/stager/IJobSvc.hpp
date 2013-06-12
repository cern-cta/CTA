/******************************************************************************
 *                castor/stager/IJobSvc.hpp
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
 * This class provides stager methods related to job handling
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_IJOBSVC_HPP
#define STAGER_IJOBSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include <vector>
#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace stager {

    // Forward declaration
    class Stream;
    class Request;
    class Segment;
    class DiskCopyInfo;
    class SvcClass;
    class FileClass;
    class SubRequest;
    class CastorFile;
    class GCLocalFile;
    class DiskCopyForRecall;

    /**
     * This class provides stager methods related to job handling
     */
    class IJobSvc : public virtual ICommonSvc {

    public:

      /**
       * Handles the start of a Get or Update job.
       * @param subreq the SubRequest to consider
       * @param diskServerName the selected diskServer
       * @param mountPoint the mounPoint of the selected fileSystem
       * @param emptyFile whether the resulting diskCopy
       * deals with the recall of an empty file
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @return the DiskCopyPath to use for the data access or
       * an empty string if the data access will have to wait
       * and there is nothing more to be done.
       * @exception Exception in case of error
       */
      virtual std::string getUpdateStart
      (castor::stager::SubRequest* subreq,
       std::string diskServerName,
       std::string mountPoint,
       bool* emptyFile,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Handles the start of a Put job.
       * @param subreq the SubRequest to consider
       * @param diskServerName the selected diskServer
       * @param mountPoint the mounPoint of the selected fileSystem
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @return the DiskCopyPath to use for the data access
       * @exception Exception in case of error
       */
      virtual std::string putStart
      (castor::stager::SubRequest* subreq,
       std::string diskServerName,
       std::string mountPoint,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      virtual void prepareForMigration
      (u_signed64 subReqId,
       u_signed64 fileSize,
       u_signed64 timeStamp,
       u_signed64 fileId,
       const std::string nsHost,
       const std::string csumtype="",
       const std::string csumvalue="")
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) was finished successfully.
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the finished SubRequest
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @exception Exception in case error
       */
      virtual void getUpdateDone
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @exception Exception in case error
       */
      virtual void getUpdateFailed
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager the a Put or a PutDone SubRequest failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @exception Exception in case error
       */
      virtual void putFailed
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager that an update subrequest has written
       * bytes into a given diskCopy. The diskCopy's status will
       * be updated to STAGEOUT and the other diskcopies of the
       * CastorFile will be invalidated
       * @param subReqId the id of the SubRequest concerned
       * @param fileId the id of the castorFile
       * @param nsHost the name server hosting this castorFile
       * @exception Exception in case of error
       */
      virtual void firstByteWritten
      (u_signed64 subRequestId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

    }; // end of class IJobSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_IJOBSVC_HPP
