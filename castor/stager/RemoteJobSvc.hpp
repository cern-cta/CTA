/******************************************************************************
 *                      RemoteJobSvc.hpp
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
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_REMOTEJOBSVC_HPP
#define STAGER_REMOTEJOBSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/stager/IJobSvc.hpp"
#include <vector>

namespace castor {

  namespace stager {

    /**
     * Constants for the configuration variables
     */
    extern const char* RMTJOBSVC_CATEGORY_CONF;
    extern const char* TIMEOUT_CONF;
    extern const int   DEFAULT_REMOTEJOBSVC_TIMEOUT;

    /**
     * Implementation of the IJobSvc for Remote stager
     */
    class RemoteJobSvc : public BaseSvc,
                         public virtual castor::stager::IJobSvc {

    public:

      /**
       * default constructor
       */
      RemoteJobSvc(const std::string name);

      /**
       * default destructor
       */
      virtual ~RemoteJobSvc() throw();

      /**
       * Get the service id
       */
      virtual inline unsigned int id() const;

      /**
       * Get the service id
       */
      static unsigned int ID();

    public:

      /**
       * Handles the start of a Get or Update job.
       * @param subreq  the SubRequest to consider
       * @param diskServerName the selected diskServer
       * @param mountPoint the mounPoint of the selected fileSystem
       * @param emptyFile whether the resulting diskCopy
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * deals with the recall of an empty file
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
        throw (castor::exception::Exception);

      /**
       * Handles the start of a Put job.
       * @param subreq  the SubRequest to consider
       * @param diskServerName the selected diskServer
       * @param mountPoint the mounPoint of the selected fileSystem
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @return the DiskCopyPath to use for the data access
       * @exception Exception in case of error
       */
      virtual std::string putStart
      (castor::stager::SubRequest* subreq,
       std::string diskServerName,
       std::string mountPoint,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception);

      /**
       * Prepares a file for migration, when needed.
       * This is called both when a stagePut is over and when a
       * putDone request is processed.
       * In the case of a stagePut that in part of a PrepareToPut,
       * it actually does not prepare the file for migration
       * but only updates its size in DB and name server.
       * Otherwise (stagePut with no prepare and putDone),
       * it also updates the filesystem free space and creates
       * the needed TapeCopies according to the FileClass of the
       * castorFile.
       * @param subReqId The id of the SubRequest to prepare
       * @param fileSize The actual size of the castor file
       * @param timeStamp The time when the size of the file is checked.
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @param csumtype The checksum type of the castor file
       * @param csumvalue The checksum value of the castor file
       * @exception Exception throws an Exception in case of error
       */

      virtual void prepareForMigration
      (u_signed64 subReqId,
       u_signed64 fileSize,
       u_signed64 timeStamp,
       u_signed64 fileId,
       const std::string nsHost,
       const std::string csumtype="",
       const std::string csumvalue="")
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) was finished successfully.
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the finished SubRequest
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception Exception throws an Exception in case of error
       */
      virtual void getUpdateDone
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception Exception throws an Exception in case of error
       */
      virtual void getUpdateFailed
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Put and PutDone SubRequest failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception Exception throws an Exception in case of error
       */
      virtual void putFailed
      (u_signed64 subReqId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception);


      /////// ICommonSvc part (not implemented)

      /**
       * Selects the next request the job service should deal with.
       * Selects a Request in START status and move its status
       * PROCESSED to avoid double processing.
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo(std::string service)
        throw (castor::exception::Exception);

      /**
       * Informs the stager that an update subrequest has written
       * bytes into a given diskCopy. The diskCopy's status will
       * be updated to STAGEOUT and the other diskcopies of the
       * CastorFile will be invalidated
       * @param subReqId the id of the SubRequest concerned
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception Exception in case of error
       */
      virtual void firstByteWritten
      (u_signed64 subRequestId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception);

    protected:

      virtual int getRemoteJobClientTimeout();


    }; // end of class RemoteJobSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_REMOTEJOBSVC_HPP
