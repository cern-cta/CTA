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
 * @(#)$RCSfile: IStagerSvc.hpp,v $ $Revision: 1.69 $ $Release$ $Date: 2006/11/30 15:38:41 $ $Author: felixehm $
 *
 * This class provides specific stager methods and includes scheduler
 * and error related methods
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ISTAGERSVC_HPP
#define STAGER_ISTAGERSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
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
       * requests of a given set of types.
       * @param types the list of accepted types for the request
       * associated to the returned subrequest
       * @return the SubRequest to process
       * @exception Exception in case of error
       */
      virtual castor::stager::SubRequest* subRequestToDo
      (std::vector<ObjectsIds> &types)
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
       * Decides whether a SubRequest should be scheduled.
       * Looks at all diskCopies for the file a SubRequest
       * deals with and depending on them, decides whether
       * to schedule the SubRequest. In case it can be scheduled,
       * also returns a list of diskcopies available to the
       * subrequest.
       * The scheduling decision is taken this way :
       * 0: no scheduling, the SubRequest is set to WAIT and it
            is linked to the parent SubRequest, which we're waiting
            on (e.g. TapeRecall, Disk2DiskCopy,..)
       * 1: schedule + list of avail sources, a DiskCopy was
            found and the SubRequest needs to be rescheduled.
       * 2: schedule + no sources, a disk2disk copy is necessary
       * 3: no schedule, no DiskCopy anywhere found, we need a
            Tape recall.
       * @param subreq the SubRequest to consider
       * @param sources this is a list of DiskCopies that
       * can be used by the subrequest.
       * Note that the DiskCopies returned in sources must be
       * deallocated by the caller.
       * @return 0,1,2,3
       * @exception Exception in case of error
       */
      virtual int isSubRequestToSchedule
      (castor::stager::SubRequest* subreq,
       std::list<castor::stager::DiskCopyForRecall*>& sources)
        throw (castor::exception::Exception) = 0;

      /**
       * Retrieves a CastorFile from the database based on its fileId
       * and name server. Creates a new one if none if found.
       * Caller is in charge of the deletion of the allocated object
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @param svcClass the service class of the castor file.
       * Used only in case of creation of a new castor file.
       * @param fileClass the file class of the castor file.
       * Used only in case of creation of a new castor file.
       * @param fileSize the size fo the castor file.
       * Used only in case of creation of a new castor file.
       * @param fileName the name of the castor file at the time
       * if this call. This will go to the DB as lastKnownFileName.
       * @return the CastorFile, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::CastorFile* selectCastorFile
      (const u_signed64 fileId, const std::string nsHost,
       u_signed64 svcClass, u_signed64 fileClass,
       u_signed64 fileSize, const std::string fileName)
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
       * possible, a null pointer is returned.
       * Else, all DiskCopies for the given file are marked
       * INVALID (that is those not in DISKCOPY_FAILED and
       * DISKCOPY_DELETED status) and all TapeCopies are
       * deleted. A new DiskCopy is then created in
       * DISKCOPY_WAITFS status, linked to the given
       * SubRequest returned.
       * Note that everything is commited and that the caller
       * is responsible for the deletion of the returned
       * DiskCopy (if any)
       * @param castorFile the file to recreate
       * @param subreq the SubRequest recreating the file
       * @return the new DiskCopy in DISKCOPY_WAITFS status
       * or null if recreation is not possible
       * @exception Exception throws an Exception in case of error
       */
      virtual castor::stager::DiskCopyForRecall* recreateCastorFile
      (castor::stager::CastorFile *castorFile,
       castor::stager::SubRequest *subreq)
        throw (castor::exception::Exception) = 0;

      /**
       * Selects a machine and FileSystem for a given job.
       * @param fileSystems the list of allowed filesystems
       * according to job requirements (given by id). This
       * is the fileSystems' mountPoint, the corresponding
       * machines are given by parameter machines.
       * A null array means that any filesystem is eligible
       * @param machines the machines on which the filesystems
       * in parameter fileSystems reside.
       * A null array means that any machine is eligible. in such
       * a case, fileSystems has to be null.
       * @param minFree the minimum free space needed on each
       * filesystem to be selected. This is filesystem dependent
       * if filesystems are given (due to possible reservations
       * of the scheduler).
       * If no filesystem are given, this array must have
       * exactely one item, used for all filesystems.
       * @param fileSystemsNb the length of the arrays
       * fileSystems, machines and minFree when they are not
       * null (and if filesystems are given for minFree)
       * @mountPoint the selected fileSystem's mountPoint
       * @diskServer the diskServer on which the selected
       * fileSystem resides.
       * @exception Exception throws an Exception in case of error
       */
      virtual void bestFileSystemForJob
      (char** fileSystems,
       char** machines,
       u_signed64* minFree,
       unsigned int fileSystemsNb,
       std::string* mountPoint,
       std::string* diskServer)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates a filesystem state (e.g : weight,
       * fsdeviation) to take into account the opening of
       * a new job.
       * @param fileSystem the file system mount point
       * @param diskServer the name of the diskserver
       * where the filesystem resides
       * @param fileSize the (supposed) size of the file
       * to be written by the job
       * @exception Exception throws an Exception in case of error
       */
      virtual void updateFileSystemForJob
      (std::string fileSystem,
       std::string diskServer,
       u_signed64 fileSize)
        throw (castor::exception::Exception) = 0;

      /**
       * Archives a SubRequest
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the SubRequest to archive
       */
      virtual void archiveSubReq(u_signed64 subReqId)
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
       * It throws a Busy exception in case the file is
       * not yet migrated. Otherwise, it deletes all
       * running requests for the file and marks all
       * the copies of the file as candidate for the
       * garbage collection.
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @exception in case of error or if the file is busy
       */
      virtual void stageRm
      (const u_signed64 fileId, const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates the gcWeight of all copies a a given file.
       * @param fileId the fileId of the CastorFile
       * @param nsHost the name server to use
       * @param weight the new gcWeight for the file
       * @exception in case of error
       */
      virtual void setFileGCWeight
      (const u_signed64 fileId, const std::string nsHost, const float weight)
        throw (castor::exception::Exception) = 0;

      /**
       * Creates a candidate for a recall. This includes TapeCopy with
       * its Segment(s), a DiskCopy and a SubRequest in WAITTAPERECALL. 
       * @param subreq the subreq of the file to recall
       * @param euid the user id
       * @param egid the group id of the user
       * @exception in case of error
       */
     virtual void createRecallCandidate
     (castor::stager::SubRequest *subreq,
      const unsigned long euid, const unsigned long egid) 
        throw (castor::exception::Exception) = 0;


    }; // end of class IStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_ISTAGERSVC_HPP
