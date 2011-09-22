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
 * @(#)$RCSfile: IStagerSvc.hpp,v $ $Revision: 1.104 $ $Release$ $Date: 2009/06/16 14:03:49 $ $Author: sponcec3 $
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
#include "castor/stager/PriorityMap.hpp"
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
    class Tape;
    class Stream;
    class Request;
    class Segment;
    class TapeCopy;
    class DiskCopy;
    class DiskPool;
    class SvcClass;
    class FileClass;
    class TapePool;
    class FileSystem;
    class DiskServer;
    class SubRequest;
    class CastorFile;
    class GCLocalFile;
    class TapeCopyForMigration;
    class DiskCopyForRecall;
    class PriorityMap;
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
       * handles a repack subRequest
       * @param subReqId the id of the repack subRequest to be handled
       * @return the result of the processing
       * @exception Exception in case of error
       */
      virtual castor::stager::BulkRequestResult* handleRepackSubRequest
      (const u_signed64 subReqId)
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
       * Retrieves a list of diskcopies for a Read job to be scheduled.
       * Looks at all diskCopies for the file a SubRequest
       * deals with and depending on them, decides whether
       * to schedule the SubRequest. In case it can be scheduled,
       * also returns a list of diskcopies available to the
       * subrequest.
       * The return value is a valid DiskCopy status and
       * the scheduling decision is taken this way :
       * -2,-3: no scheduling, subrequest put on WAITSUBREQ status.
       * -1: no scheduling because of user error.
       * DISKCOPY_STAGED (0): schedule + list of avail sources,
         a DiskCopy was found and the SubRequest can be scheduled.
       * DISKCOPY_WAITDISK2DISKCOPY (1): like above, plus the
       * stager is allowed to replicate according to policies.
       * DISKCOPY_WAITTAPERECALL (2): no scheduling, no DiskCopy
         found anywhere, a tape recall is needed.
       * DISKCOPY_WAITFS (5): the only available DiskCopy is in
       * WAITFS, i.e. this is an Update inside PrepareToPut:
       * recreateCastorFile is to be called in such a case.
       * @param subreq the SubRequest to consider
       * @param sources this is a list of DiskCopies that
       * can be used by the subrequest.
       * Note that the DiskCopies returned in sources must be
       * deallocated by the caller.
       * @return -3,-2,-1,0,1,2,5
       * @exception Exception in case of system error
       */
      virtual int getDiskCopiesForJob
      (castor::stager::SubRequest* subreq,
       std::list<castor::stager::DiskCopyForRecall*>& sources)
        throw (castor::exception::Exception) = 0;

      /**
       * Processes a PToGet, PToUpdate, or Repack subrequest.
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
       * Recreates a castorFile.
       * Depending on the context, this method cleans up the
       * database when a castor file is recreated or gets
       * the unique DiskCopy of a castor file.
       * When called in the context of a Put inside a
       * PrepareToPut, the method returns the unique DiskCopy
       * associated to the castorFile. This DiskCopy can be
       * either in WAITFS, WAITFS_SCHEDULING or STAGEOUT
       * status and is linked to the SubRequest.
       * In all others cases, the method first
       * checks whether the recreation is possible.
       * A recreation is considered to be possible if
       * no TapeCopy of the given file is in TAPECOPY_SELECTED
       * status and no DiskCopy of the file is in either
       * WAITFS, WAITFS_SCHEDULING, WAITTAPERECALL or
       * WAITDISK2DISKCOPY status. When recreation is not
       * possible, a null pointer is returned, with the exception
       * of WAITFS_SCHEDULING, where a DiskCopy is still returned
       * for logging purposes.
       * Else, all DiskCopies for the given file are marked
       * INVALID (that is those not in DISKCOPY_FAILED and
       * DISKCOPY_DELETED status) and all TapeCopies are
       * deleted. A new DiskCopy is then created in
       * DISKCOPY_WAITFS status, linked to the given
       * SubRequest returned.
       * Note that the caller is responsible for the
       * deletion of the returned DiskCopy (if any)
       * @param castorFile the file to recreate
       * @param subreq the SubRequest recreating the file
       * @return the new DiskCopy in DISKCOPY_WAITFS|DISKCOPY_WAITFS_SCHEDULING
       * status, or null if recreation is not possible.
       * @exception Exception throws an Exception in case of error
       */
      virtual castor::stager::DiskCopyForRecall* recreateCastorFile
      (castor::stager::CastorFile *castorFile,
       castor::stager::SubRequest *subreq)
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
       * Implements a single file stageRelease.
       * It throws a Busy exception in case the file is
       * used by any request or is waiting for migration.
       * Otherwise, it marks all the copies of the file
       * as candidate for the garbage collection.
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception in case of error or if the file is busy
       */
      virtual void stageRelease
      (const u_signed64 fileId, const std::string nsHost)
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
       * A proper diskcopy in status STAGED will be created
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
       * Creates a candidate for a recall. This includes TapeCopy with
       * its Segment(s), a DiskCopy and a SubRequest in WAITTAPERECALL.
       * @param subreq the subreq of the file to recall
       * @param svcClass svc class for recall policy
       * @param tape a pointer to a location of where to store the tape
       * information associated with the recall. In case the pointer is
       * not empty, this should be considered as an hint of the tape that
       * should be used for the recall, if possible. Note: if the file has
       * multiple segments spread across multiple tapes only the last
       * Tape processed will be returned. We could of course return a
       * vector of Tape objects but this is overkill as multi segment,
       * multi tape recalls are extremely rare and can only happen when
       * recalling a file which was written under Castor1. It is the
       * responsibility of the calling function to delete the Tape object
       * @return 0: error (e.g. no valid segments)
       *         1: success
       * @exception Exception in case of error.
       */
     virtual int createRecallCandidate
     (castor::stager::SubRequest* subreq,
      castor::stager::SvcClass* svcClass,
      castor::stager::Tape* &tape)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a DiskPool from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the disk pool
       * @return the DiskPool object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskPool* selectDiskPool
      (const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a TapePool from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the tape pool
       * @return the TapePool object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::TapePool* selectTapePool
      (const std::string name)
        throw (castor::exception::Exception) = 0;

      /**
       * Select priority for recall
       * @param euid the userid of the user
       * @param egid the groupid of the user
       * @return priority value
       * @exception in case of an error
       */
      virtual std::vector<castor::stager::PriorityMap*>
      selectPriority(int euid, int egid, int priority)
        throw (castor::exception::Exception) = 0;

      /**
       * Enter priority for recall
       * @param euid the userid of the user
       * @param egid the groupid of the user
       * @param priority value
       * @exception in case of an error
       */
      virtual void enterPriority(u_signed64 euid,
                                 u_signed64 egid,
                                 u_signed64 priority)
        throw (castor::exception::Exception) = 0;

      /**
       * Delete priority for recall
       * @param euid the userid of the user
       * @param egid the groupid of the user
       * @exception in case of an error
       */
      virtual void deletePriority(int euid, int egid)
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

    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
