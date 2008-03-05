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
 * @(#)$RCSfile: IJobSvc.hpp,v $ $Revision: 1.13 $ $Release$ $Date: 2008/03/05 16:14:32 $ $Author: riojac3 $
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
    class Tape;
    class Stream;
    class Request;
    class Segment;
    class TapeCopy;
    class DiskCopy;
    class DiskCopyInfo;
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
     * This class provides stager methods related to job handling
     */
    class IJobSvc : public virtual ICommonSvc {

    public:

      /**
       * Handles the start of a Get or Update job.
       * Schedules the corresponding SubRequest on a given
       * FileSystem and returns the DiskCopy to use for data
       * access.
       * Note that deallocation of the DiskCopy is the
       * responsability of the caller.
       * Depending on the available DiskCopies for the file
       * the SubRequest deals with, we have different cases :
       *  - no DiskCopy at all and file is not of size 0 :
       * a DiskCopy is created with status DISKCOPY_WAITTAPERECALL.
       * Null pointer is returned
       *  - no DiskCopy at all and file is of size 0 :
       * a DiskCopy is created with status DISKCOPY_WAIDISK2DISKCOPY.
       * This diskCopy is returned and the emptyFile content is
       * set to true.
       *  - one DiskCopy in DISKCOPY_WAITTAPERECALL, DISKCOPY_WAITFS
       * or DISKCOPY_WAITDISK2DISKCOPY status :
       * the SubRequest is linked to the one recalling and
       * put in SUBREQUEST_WAITSUBREQ status. Null pointer is
       * returned.
       *  - no valid (STAGE*, WAIT*) DiskCopy on the selected
       * FileSystem but some in status DISKCOPY_STAGEOUT or
       * DISKCOPY_STAGED on other FileSystems : a new DiskCopy
       * is created with status DISKCOPY_WAITDISK2DISKCOPY.
       * It is returned and the sources parameter is filed
       * with the DiskCopies found on the non selected FileSystems.
       *  - one DiskCopy on the selected FileSystem in
       * DISKCOPY_STAGEOUT or DISKCOPY_STAGED status :
       * the SubRequest is ready, the DiskCopy is returned and
       * sources remains empty.
       * @param subreq  the SubRequest to consider
       * @param fileSystem the selected FileSystem
       * @param emptyFile whether the resulting diskCopy
       * @param fileId
       * @param nsHost
       * deals with the recall of an empty file
       * @return the DiskCopy to use for the data access or
       * a null pointer if the data access will have to wait
       * and there is nothing more to be done. Even in case
       * of a non null pointer, the data access will have to
       * wait for a disk to disk copy if the returned DiskCopy
       * is in DISKCOPY_WAITDISKTODISKCOPY status. This
       * disk to disk copy is the responsability of the caller.
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskCopy* getUpdateStart
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem,
       bool* emptyFile,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Same as above, with the old signature for backward compatibility.
       * This method is DEPRECATED and should be removed at some stage
       */
  /*    virtual castor::stager::DiskCopy* getUpdateStart
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem,
       std::list<castor::stager::DiskCopyForRecall*>& sources,
       bool* emptyFile)
        throw (castor::exception::Exception) = 0;
*/
      /**
       * Handles the start of a Put job.
       * Links the DiskCopy associated to the SubRequest to
       * the given FileSystem and updates the DiskCopy status
       * to DISKCOPY_STAGEOUT.
       * Note that deallocation of the DiskCopy is the
       * responsability of the caller.
       * @param subreq  the SubRequest to consider
       * @param fileSystem the selected FileSystem
       * @param fileId
       * @param nsHost
       * @return the DiskCopy to use for the data access
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskCopy* putStart
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Handles the start of a StageDiskCopyReplicaRequest. It checks
       * that the source DiskCopy stills exists i.e. hasn't been 
       * garbage collected. Updates the filesystem of the destination
       * DiskCoy and verifies that the selected destination diskserver
       * and filesystem are valid for the given service class.
       * @param diskcopyId the id of the new DiskCopy
       * @param sourceDiskCopyId the id of the source diskCopy
       * @param destSvcClass the service class of the diskserver writing
       * the new castor file.
       * @param diskServer the name of the destination diskserver
       * @param fileSystem the file system mount point
       * @param fileId
       * @param nsHost
       * Changes are commited
       * @return diskCopy information about the destination DiskCopy
       * @return sourceDiskCopy information about the source DiskCopy
       * @exception Exception in case of error
       */
      virtual void disk2DiskCopyStart
      (const u_signed64 diskCopyId,
       const u_signed64 sourceDiskCopyId,
       const std::string destSvcClass,
       const std::string diskServer,
       const std::string fileSystem,
       castor::stager::DiskCopyInfo* &diskCopy,
       castor::stager::DiskCopyInfo* &sourceDiskCopy,
       u_signed64 fileId,
       const std::string nsHost)
       
        throw(castor::exception::Exception) = 0;
      
      /**
       * Updates database after successful completion of a
       * disk to disk copy. This includes setting the DiskCopy
       * status to DISKCOPY_STAGED and setting the SubRequest
       * status to SUBREQUEST_READY.
       * Changes are commited
       * @param diskcopyId the id of the new DiskCopy
       * @param sourceDiskCopyId the id of the source diskCopy
       * @param fileId
       * @param nsHost
       * @exception Exception in case of error
       */
      virtual void disk2DiskCopyDone
      (u_signed64 diskCopyId,
       u_signed64 sourceDiskCopyId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Updates database after a failure of a disk to disk copy.
       * Changes are commited
       * @param diskcopyId the id of the failed DiskCopy
       * @param fileId
       * @param nsHost
       * @exception Exception in case of error
       */
      virtual void disk2DiskCopyFailed
      (u_signed64 diskCopyId,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

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
       * @param subreq The SubRequest handling the file to prepare
       * @param fileSize The actual size of the castor file
       * @param timeStamp To know if the fileSize is still valid
       * @param fileId
       * @param nsHost
       * @exception Exception in case of error
       */
      virtual void prepareForMigration
      (castor::stager::SubRequest* subreq,
       u_signed64 fileSize,
       u_signed64 timeStamp,
       u_signed64 fileId,
       const std::string nsHost)
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) was finished successfully.
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the finished SubRequest
       * @param fileId
       * @param nsHost
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
       * @param fileId
       * @param nsHost
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
       * @param fileId
       * @param nsHost
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
       * @param fileId
       * @param nsHost
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
