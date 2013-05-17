/******************************************************************************
 *                castor/stager/IStagerSvc.hpp
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
 * This class provides specific stager methods and includes scheduler
 * and error related methods
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ISTAGERSVC_HPP
#define STAGER_ISTAGERSVC_HPP 1

// Include Files
#include "Cns_api.h"
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
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
    class SvcClass;
    class FileClass;
    class SubRequest;
    class CastorFile;
    class GCLocalFile;
    class DiskCopyForRecall;
    class BulkRequestResult;

    /**
     * This class provides specific stager methods and includes
         * scheduler and error related methods
     */
    class IStagerSvc : public virtual ICommonSvc {

    public:

      /**
       * Selects the next SubRequest the stager should deal with.
       * Selects a SubRequest in START, RESTART or RETRY status
       * and move its status to SUBREQUEST_WAITSCHED to avoid
       * double processing.
       * The selection is restricted to SubRequest associated to
       * requests of a given set of types, identified by service.
       * @param service identifies the group of requests to be queried
       * @return the SubRequest to process
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequest* subRequestToDo
      (const std::string service)
        throw (castor::exception::Exception) = 0;

      /**
       * Selects and processes a bulk request.
       * @param service identifies the group of bulk requests targetted
       * @return the result of the processing
       * @exception Exception in case of error
       */
      virtual castor::IObject* processBulkRequest
      (const std::string service)
        throw (castor::exception::Exception) = 0;

      /**
       * Selects the next SubRequest in FAILED status the stager
       * should deal with.
       * Selects a SubRequest in FAILED status and move its status
       * to FAILED_ANSWERING to avoid double processing.
       * @return the SubRequest to process
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequest* subRequestFailedToDo()
        throw (castor::exception::Exception) = 0;

      /**
       * Processes a PToGet and PToUpdate subrequest.
       * @param subreq the SubRequest to consider
       * @return -2,-1,0,1,2, cf. return value for getDiskCopiesForJob.
       * @exception Exception in case of system error
       */
      virtual int processPrepareRequest
      (castor::stager::SubRequest* subreq)
        throw (castor::exception::Exception) = 0;

      /**
       * Processes a putDone subrequest.
       * @param subreq the SubRequest to consider
       * @return -1: putDone put in wait on a running put.
       *          0: user error, putDone cannot be performed.
       *          1: success.
       * @exception Exception in case of system error
       */
      virtual int processPutDoneRequest
      (castor::stager::SubRequest* subreq)
        throw (castor::exception::Exception) = 0;

      /**
       * Create an internal request to trigger a diskcopy replication.
       * @param subreq the SubRequest which has to wait for the replication
       * @param srcDiskCopy the source diskCopy
       * @param srcSc the source service class
       * @param destSc the destination service class
       * @param internal flag to indicate whether this is an internal
       * replication request.
       * @exception Exception in case of system error
       */
      virtual void createDiskCopyReplicaRequest
      (const castor::stager::SubRequest* subreq,
       const castor::stager::DiskCopyForRecall* srcDiskCopy,
       const castor::stager::SvcClass* srcSc,
       const castor::stager::SvcClass* destSc,
       const bool internal = false)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a CastorFile from the database based on its fileId
       * and name server. Creates a new one if none if found.
       * Caller is in charge of the deletion of the allocated object
       * @param subreq the subRequest concerning this file
       * @param cnsFileId the NS fileid structure of the CastorFile
       * @param cnsFilestat the NS filestat structure of the file
       * Used only in case of creation of a new castor file.
       * @return the CastorFile instance.
       * @exception Exception in case of error
       */
      virtual castor::stager::CastorFile* selectCastorFile
        (castor::stager::SubRequest* subreq,
         const Cns_fileid* cnsFileId, const Cns_filestatcs* cnsFileStat)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves the location of the best diskcopy to read from
       * given by the castorfile and service class where the file
       * is requested.
       * @param castorFile the file to lookup
       * @param svcClass the service class where the file is requested
       * @return The information about the diskcopy or 0 if none is
       * found. Note: not all attributes of the DiskCopyInfo class are
       * provided.
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskCopyInfo* getBestDiskCopyToRead
      (const castor::stager::CastorFile *castorFile,
       const castor::stager::SvcClass *svcClass)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates a SubRequest status in the DB, including
       * the answered flag that is set to 1 and tells
       * whether the request to which it belongs still
       * has some other SubRequests that were not processed.
       * By not processed we mean that their "answered" flag
       * is not set AND their status is neither READY neither
       * FINISHED nor one of the FAILED* status.
       * The two operations are executed atomically.
       * The update is commited before returning.
       * This method should only be called when the calling
       * process is answering to the client. In other cases,
       * the updateRep method should be used.
       * @param subreq the SubRequest to update
       * @return whether there are still SubRequests in
       * SUBREQUEST_START status within the same request
       * @exception Exception in case of error
       */
      virtual bool updateAndCheckSubRequest
      (castor::stager::SubRequest *subreq)
        throw (castor::exception::Exception) = 0;

      /**
       * Archives a SubRequest
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the SubRequest to archive
       * @param finalStatus the final status of the SubRequest,
       * either 8 (FINISHED) or 9 (FAILED_FINISHED)
       */
      virtual void archiveSubReq(u_signed64 subReqId,
        castor::stager::SubRequestStatusCodes finalStatus)
        throw (castor::exception::Exception) = 0;

      /**
       * Implements a single file stageRm.
       * It does nothing in case the file is not yet
       * migrated, or a recall or replica are ongoing.
       * Otherwise, it deletes all running requests
       * for the file and marks all the copies of the file
       * as candidates for the garbage collection.
       * If fileId is not found, a stageRm
       * is still performed in the stager if nothing is
       * found on the nameserver for this fileName.
       * @param subreq the subRequest to be processed
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @param svcClassId the svcClass where to perform
       * the rm operation; 0 for all svcClasses.
       * @return 0: user error (ENOENT)
       *         1: success.
       * @exception in case of system error
       */
      virtual int stageRm
      (castor::stager::SubRequest* subreq, const u_signed64 fileId,
       const std::string nsHost, const u_signed64 svcClassId)
        throw (castor::exception::Exception) = 0;

      /*
       * cleanups the stager DB in case of a stagerRm for a renamed file
       * Handles accordingly the subrequest (failing it if needed)
       */
      virtual void renamedFileCleanup
      (const std::string &fileName, const u_signed64 subReqId)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates the gcWeight of some copies of a given file.
       * The passed weight (in seconds) is added to the current one,
       * resulting in the diskcopies being 'younger'.
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @param svcClassId the service class id to be affected
       * @param weight the new gcWeight for the file
       * @return 0: user error (ENOENT)
       *         1: success.
       * @exception in case of error
       */
      virtual int setFileGCWeight
      (const u_signed64 fileId, const std::string nsHost,
       const u_signed64 svcClassId, const float weight)
        throw (castor::exception::Exception) = 0;

      /**
       * Creates an empty (i.e. 0 size) file in the DB.
       * A proper diskcopy in status VALID will be created
       * and the subrequest will be updated to schedule an
       * access to the file if requested
       * @param subreq the subreq of the file to recall
       * @param schedule whether to update the subrequest to
       * schedule an access to the file
       * @exception Exception in case of error
       */
      virtual void createEmptyFile
      (castor::stager::SubRequest* subreq,
       bool schedule)
        throw (castor::exception::Exception) = 0;

      /**
       * Triggers the recall of a file
       * @param srId the id of the subRequest triggering the recall
       * @return the status of the subrequest after the call (WAITTAPERECALL OR FAILED)
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequestStatusCodes createRecallCandidate(u_signed64 srId)
        throw (castor::exception::Exception) = 0;

      /**
       * Gets a configuration option from the
       * CastorConfig table
       * @param class a string containing the
       * option class (e.g. stager)
       * @param key a string containing the
       * option key to be accessed
       * @param defaultValue a default return
       * value when the key is not found
       * @return the option value
       * @exception in case of an error
       */
      virtual std::string getConfigOption(std::string confClass,
                                          std::string confKey,
                                          std::string defaultValue)
        throw (castor::exception::Exception) = 0;

      /**
       * handles a get request
       * @param cfId the id of the castorFile concerned
       * @param srId the id of the subrequest concerned
       * @param fileId the fileId of the file concerned
       * @param fileSize the size of the file concerned
       */
      virtual void handleGet(u_signed64 cfId,
                             u_signed64 srId,
                             struct Cns_fileid &fileId,
                             u_signed64 fileSize)
        throw (castor::exception::Exception) = 0;

      /**
       * handles a prepareToGet request and returns a subrequest's status
       * indicating whether we are going for a recall/d2dcopy (WAITTAPERECALL),
       * whether we failed (FAILED) or whether we foudn the file (FINISHED)
       * @param cfId the id of the castorFile concerned
       * @param srId the id of the subrequest concerned
       * @param fileId the fileId of the file concerned
       * @param fileSize the size of the file concerned
       */
      virtual castor::stager::SubRequestStatusCodes
      handlePrepareToGet(u_signed64 cfId,
                         u_signed64 srId,
                         struct Cns_fileid &fileId,
                         u_signed64 fileSize)
        throw (castor::exception::Exception) = 0;

      /**
       * handles a put request
       * @param cfId the id of the castorFile concerned
       * @param srId the id of the subrequest concerned
       * @param fileId the fileId of the file concerned
       */
      virtual void handlePut(u_signed64 cfId,
                             u_signed64 srId,
                             struct Cns_fileid &fileId)
        throw (castor::exception::Exception) = 0;

      /**
       * handles a prepareToPut request and returns a boolean indicating
       * whether we need to reply to the client or not
       * @param cfId the id of the castorFile concerned
       * @param srId the id of the subrequest concerned
       * @param fileId the fileId of the file concerned
       * @param fileSize the size of the file concerned
       */
      virtual bool handlePrepareToPut(u_signed64 cfId,
                                      u_signed64 srId,
                                      struct Cns_fileid &fileId)
        throw (castor::exception::Exception) = 0;

      /**
       * Dumps the current logs from the db.
       * The content is logged in DLF and then deleted.
       */
      virtual void dumpDBLogs()
        throw (castor::exception::Exception) = 0;
      
    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
