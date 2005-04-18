/******************************************************************************
 *                      RemoteStagerSvc.hpp
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
 * @(#)$RCSfile: RemoteStagerSvc.hpp,v $ $Revision: 1.35 $ $Release$ $Date: 2005/04/18 09:18:15 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_REMOTESTAGERSVC_HPP
#define STAGER_REMOTESTAGERSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include <vector>

namespace castor {

  namespace stager {

    /**
     * Constants for the configuration variables
     */
    extern const char* RMTSTGSVC_CATEGORY_CONF;
    extern const char* TIMEOUT_CONF;
    extern const int   DEFAULT_REMOTESTGSVC_TIMEOUT;

    /**
     * Implementation of the IStagerSvc for Oracle
     */
    class RemoteStagerSvc : public BaseSvc,
                            public virtual castor::stager::IStagerSvc {

    public:

      /**
       * default constructor
       */
      RemoteStagerSvc(const std::string name);

      /**
       * default destructor
       */
      virtual ~RemoteStagerSvc() throw();

      /**
       * Get the service id
       */
      virtual inline const unsigned int id() const;

      /**
       * Get the service id
       */
      static const unsigned int ID();

    public:

      /**
       * Check if there still are any segments waiting for a given tape.
       * Before a tape is physically mounted, the VidWorker process will
       * check if there still are Segments entries waiting for this tape.
       * If not, the tape request is cancelled. If there is at least one
       * matching entry, the matching catalog entries Tape status should be
       * updated to TAPE_WAITMOUNT before return.
       * TAPE_WAITMOUNT indicates that the tape request will continue
       * mounting the tape and the matching Segments entries should normally
       * wait for this tape to be mounted. This means that if the CASTOR
       * file has multiple tape copies, the tape requests for the other
       * copies should be cancelled unless there are outstanding requests
       * for other files that reside on that tape.
       * @param searchItem the tape information used for the search
       * @return >0 : number of waiting requests found. 0 : no requests found
       * @exception in case of error
       */
      virtual int anySegmentsForTape(castor::stager::Tape* searchItem)
        throw (castor::exception::Exception);

      /*
       * Get the array of segments currently waiting for a given tape.
       * Search the catalog for all eligible segments that
       * are waiting for the given tape VID to become ready.
       * The matching Segments entries must have the status
       * SEGMENT_UNPROCESSED.
       * Before return this function atomically updates the
       * matching catalog entries Tape status to TAPE_MOUNTED
       * and the segments to SEGMENT_SELECTED.
       * @param searchItem the tape information used for the search
       * @return vector with all waiting segments
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Segment*>
      segmentsForTape(castor::stager::Tape* searchItem)
        throw (castor::exception::Exception);

      /**
       * Finds the best filesystem for a given segment.
       * Looks for a filesystem where to write the segment content
       * once it will be retrieved from tape. This file system
       * must have enough space and the one with the biggest weight
       * will be taken (if any).
       * If a filesystem is chosen, then the link with the only
       * DiskCopy available for the CastorFile the segment belongs
       * to is created.
       * @param segment the segment we are dealing with
       * @return The only DiskCopy available for the CastorFile the
       * segment belongs too. A DiskCopyForRecall is actually returned
       * that contains additionnal information. The Castorfile associated
       * is also created
       * @exception in case of error
       */
      virtual castor::stager::DiskCopyForRecall* bestFileSystemForSegment
      (castor::stager::Segment *segment)
        throw (castor::exception::Exception);

      /**
       * Check if there still is any tapeCopy waiting for a stream.
       * The matching TapeCopies entry must have the status
       * TAPECOPY_WAITINSTREAM. If there is at least one, the Stream
       * status is updated to STREAM_WAITMOUNT before return. This
       * indicates that the stream will continue mounting the tape.
       * @param searchItem the stream information used for the search
       * @return whether a Tapecopy is waiting
       * @exception in case of error
       */
      virtual bool anyTapeCopyForStream(castor::stager::Stream* searchItem)
        throw (castor::exception::Exception);

      /*
       * Get the best TapeCopy currently waiting for a given Stream.
       * Search the catalog for the best eligible TapeCopy that
       * is waiting for the given Stream to become ready.
       * The matching TapeCopy entry must have the status
       * TAPECOPY_WAITINSTREAMS and will be changed to status SELECTED.
       * Before return this function atomically updates the
       * matching catalog entry Stream status to STREAM_RUNNING.
       * The changes in the database are automatically commited
       * @param searchItem the Stream information used for the search
       * @return the best waiting TapeCopy (or 0 if none).
       * @exception in case of error
       */
      virtual castor::stager::TapeCopyForMigration*
      bestTapeCopyForStream(castor::stager::Stream* searchItem)
        throw (castor::exception::Exception);

      /*
       * Gets the streams associated to the given TapePool
       * and link them to the pool. Takes a lock on the
       * returned streams in the database and does not 
       * commit.
       * @param tapePool the tapePool to handle
       * @exception in case of error
       */
      virtual void streamsForTapePool
      (castor::stager::TapePool* tapePool)
        throw (castor::exception::Exception);

      /**
       * Updates the database when a file recall is successfully over.
       * This includes updating the DiskCopy status to DISKCOPY_STAGED
       * (note that it is guaranteed that there is a single
       * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
       * It also includes updating the status of the corresponding
       * SubRequest to SUBREQUEST_RESTART and updating the status of
       * the SubRequests waiting on this recall to SUBREQUEST_RESTART
       * @param tapeCopy the TapeCopy that was just recalled
       * @exception in case of error
       */
      virtual void fileRecalled(castor::stager::TapeCopy* tapeCopy)
        throw (castor::exception::Exception);

      /**
       * Updates the database when a file recall failed.
       * This includes updating the DiskCopy status to DISKCOPY_FAILED
       * (note that it is garanted that there is a single
       * diskcopy in status DISKCOPY_WAITTAPERECALL for this TapeCopy).
       * It also includes updating the status of the corresponding
       * SubRequest to SUBREQUEST_FAILED and updating the status of
       * the SubRequests waiting on this recall to SUBREQUEST_FAILED
       * @param tapeCopy the TapeCopy that was just recalled
       * @exception in case of error
       */
      virtual void fileRecallFailed(castor::stager::TapeCopy* tapeCopy)
        throw (castor::exception::Exception);

      /**
       * Get an array of the tapes to be processed.
       * This method searches the request catalog for all tapes that are
       * in TAPE_PENDING status. It atomically updates the status to
       * TAPE_WAITVDQM and returns the corresponding Tape objects.
       * This means that a subsequent call to this method will not return
       * the same entries. Objects may be present n times in the returned
       * vector of tapes. The rtcpclientd will notice multiple identical
       * requests and only submit one of them to VDQM.
       * @return vector of tapes to be processed
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Tape*> tapesToDo()
        throw (castor::exception::Exception);

      /**
       * Get an array of the streams to be processed.
       * This method searches the catalog for all streams that are
       * in STREAM_PENDING status. It atomically updates the status to
       * STREAM_WAITDRIVE and returns the corresponding Stream objects.
       * This means that a subsequent call to this method will not return
       * the same entries.
       * @return vector of streams to be processed
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Stream*> streamsToDo()
        throw (castor::exception::Exception);

      /**
       * Retrieves a tape from the database based on its vid,
       * side and tpmode. If no tape is found, creates one
       * Note that this method creates a lock on the row of the
       * given tape and does not release it. It is the
       * responsability of the caller to commit the transaction.
       * @param vid the vid of the tape
       * @param side the side of the tape
       * @param tpmode the tpmode of the tape
       * @return the tape. the return value can never be 0
       * @exception Exception in case of error (no tape found,
       * several tapes found, DB problem, etc...)
       */
      castor::stager::Tape* selectTape(const std::string vid,
                                       const int side,
                                       const int tpmode)
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);

      /**
       * Selects the next request the stager should deal with.
       * Selects a Request in START status and move its status
       * PROCESSED to avoid double processing.
       * The selection is restricted to Request of a given set
       * of types.
       * @param types the list of accepted types for the request
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo
      (std::vector<ObjectsIds> &types)
        throw (castor::exception::Exception);

      /**
       * Decides whether a SubRequest should be scheduled.
       * Looks at all diskCopies for the file a SubRequest
       * deals with and depending on them, decides whether
       * to schedule the SubRequest. In case it can be scheduled,
       * also returns a list of diskcopies available to the
       * subrequest.
       * The scheduling decision is taken this way :
       *   - if no diskCopy is found, return true (scheduling
       * for tape recall) and sources stays empty.
       *   - if some diskcopies are found but all in WAIT*
       * status, return false (no schedule) and link the SubRequest
       * to the one we're waiting on + set its status to
       * SUBREQUEST_WAITSUBREQ. Sources stays empty.
       *   - if some diskcopies are found in STAGED/STAGEOUT
       * status, return true and list them in sources.
       * @param subreq the SubRequest to consider
       * @param sources this is a list of DiskCopies that
       * can be used by the subrequest.
       * Note that the DiskCopies returned in sources must be
       * deallocated by the caller.
       * @return whether to schedule it
       * @exception Exception in case of error
       */
      virtual bool isSubRequestToSchedule
      (castor::stager::SubRequest* subreq,
       std::list<castor::stager::DiskCopyForRecall*>& sources)
        throw (castor::exception::Exception);

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
       * @param sources this is a list of DiskCopies that
       * can be used as source of a Disk to Disk copy. This
       * list is never empty when diskCopy has status
       * DISKCOPY_DISK2DISKCOPY and always empty otherwise.
       * Note that the DiskCopies returned in sources must be
       * deallocated by the caller.
       * @param emptyFile whether the resulting diskCopy
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
       std::list<castor::stager::DiskCopyForRecall*>& sources,
       bool* emptyFile)
        throw (castor::exception::Exception);

      /**
       * Handles the start of a Put job.
       * Links the DiskCopy associated to the SubRequest to
       * the given FileSystem and updates the DiskCopy status
       * to DISKCOPY_STAGEOUT.
       * Note that deallocation of the DiskCopy is the
       * responsability of the caller.
       * @param subreq  the SubRequest to consider
       * @param fileSystem the selected FileSystem
       * @return the DiskCopy to use for the data access
       * @exception Exception in case of error
       */      
      virtual castor::stager::DiskCopy* putStart
      (castor::stager::SubRequest* subreq,
       castor::stager::FileSystem* fileSystem)
        throw (castor::exception::Exception);

      /**
       * Retrieves a SvcClass from the database based on its name.
       * Caller is in charge of the deletion of the allocated object
       * @param name the name of the SvcClass
       * @return the SvcClass, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::SvcClass* selectSvcClass(const std::string name)
        throw (castor::exception::Exception);

      /**
       * Retrieves a FileClass from the database based on its name.
       * Caller is in charge of the deletion of the allocated object
       * @param name the name of the FileClass
       * @return the FileClass, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::FileClass* selectFileClass(const std::string name)
        throw (castor::exception::Exception);

      /**
       * Retrieves a CastorFile from the database based on its fileId.
       * Caller is in charge of the deletion of the allocated object
       * @param fileId the fileId of the CastorFile
       * @param svcClass the service class of the castor file.
       * Used only in case of creation of a new castor file.
       * @param fileClass the file class of the castor file.
       * Used only in case of creation of a new castor file.
       * @param fileSize the size fo the castor file.
       * Used only in case of creation of a new castor file.
       * @return the CastorFile, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::CastorFile* selectCastorFile
      (const u_signed64 fileId, const std::string nsHost,
       u_signed64 svcClass, u_signed64 fileClass,
       u_signed64 fileSize)
        throw (castor::exception::Exception);

      /**
       * Retrieves a FileSystem from the database based on its
       * mount point and diskServer name.
       * Caller is in charge of the deletion of the allocated
       * objects, including the DiskServer Object
       * @param mountPoint the mountPoint of the FileSystem
       * @param diskServer the name of the disk server hosting this file system
       * @return the FileSystem linked to its DiskServer, or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::FileSystem* selectFileSystem
      (const std::string mountPoint,
       const std::string diskServer)
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);

      /**
       * Retrieves a DiskServer from the database based on name.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param name the name of the disk server
       * @return the DiskServer object or 0 if none found
       * @exception Exception in case of error
       */
      virtual castor::stager::DiskServer* selectDiskServer
      (const std::string name)
        throw (castor::exception::Exception);

      /**
       * Retrieves the TapeCopies from the database that have
       * status TAPECOPY_CREATED or TAPECOPY_TOBEMIGRATED and
       * have a castorFile linked to the right SvcClass.
       * Changes their status to TAPECOPY_WAITINSTREAMS.
       * Caller is in charge of the deletion of the allocated
       * memory.
       * @param svcClass the SvcClass we select on
       * @return a vector of matching TapeCopies
       * @exception Exception in case of error
       */
      virtual std::vector<castor::stager::TapeCopy*>
      selectTapeCopiesForMigration
      (castor::stager::SvcClass *svcClass)
        throw (castor::exception::Exception);

      /**
       * Updates a SubRequest status in the DB and tells
       * whether the request to which it belongs still
       * has some SubRequests in SUBREQUEST_START status.
       * The two operations are executed atomically.
       * The update is commited before returning.
       * @param subreq the SubRequest to update
       * @return whether there are still SubRequests in
       * SUBREQUEST_START status within the same request
       * @exception Exception in case of error
       */
      virtual bool updateAndCheckSubRequest
      (castor::stager::SubRequest *subreq)
        throw (castor::exception::Exception);

      /**
       * Updates database after successful completion of a
       * disk to disk copy. This includes setting the DiskCopy
       * status to DISKCOPY_STAGED and setting the SubRequest
       * status to SUBREQUEST_READY.
       * Changes are commited
       * @param diskcopyId the id of the new DiskCopy
       * @param status the status of the new DiskCopy
       * @exception Exception throws an Exception in case of error
       */
      virtual void disk2DiskCopyDone
      (u_signed64 diskCopyId,
       castor::stager::DiskCopyStatusCodes status)
        throw (castor::exception::Exception);

      /**
       * Recreates a castorFile. This method cleans up the
       * database when a castor file is recreated. It first
       * checks whether the recreation is possible.
       * A recreation is considered to be possible if
       * no TapeCopy of the given file is in TAPECOPY_SELECTED
       * status and no DiskCopy of the file is in either
       * DISKCOPY_WAITFS or DISKCOPY_WAITTAPERECALL or
       * DISKCOPY_WAITDISK2DISKCOPY status.
       * When recreation is not possible, a null pointer is
       * returned.
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
      virtual castor::stager::DiskCopy* recreateCastorFile
      (castor::stager::CastorFile *castorFile,
       castor::stager::SubRequest *subreq)
        throw (castor::exception::Exception);

      /**
       * Prepares a file for migration. This is called
       * when a put is over. It involves updating the file
       * size to the actual value, archiving the subrequest
       * and calling putDone.
       * @param subreq The SubRequest handling the file to prepare
       * @param fileSize The actual size of the castor file
       * @exception Exception throws an Exception in case of error
       */
      virtual void prepareForMigration
      (castor::stager::SubRequest* subreq,
       u_signed64 fileSize)
        throw (castor::exception::Exception);

      /**
       * Implementation of the PutDone API. This is called
       * when a PrepareToPut is over. It involves
       * creating the needed TapeCopies according to the
       * FileClass of the castorFile.
       * @param cfId The id of the CastorFile concerned
       * @param fileSize The actual size of the castor file.
       * This is only used to detect 0 length files
       * @exception Exception throws an Exception in case of error
       */
      virtual void putDone (u_signed64 cfId,
                            u_signed64 fileSize)
        throw (castor::exception::Exception);

      /**
       * resets a stream by either deleting it or setting
       * its status to STREAM_PENDING depending on whether
       * there are TapeCopies in status WAITINSTREAMS status.
       * Also deletes all links to TapeCopies for this stream
       * @param stream the stream to reset
       * @exception Exception throws an Exception in case of error
       */
      virtual void resetStream(castor::stager::Stream* stream)
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);

      /**
       * List files to be deleted on a given diskServer.
       * These are the files corresponding to DiskCopies
       * in GCCANDIDATE status. This status is changed
       * to BEINGDELETED atomically.
       * @param diskServer the name of the DiskServer
       * involved
       * @return a list of files. The id of the DiskCopy
       * is given as well as the local path on the server.
       * Note that the returned vector should be deallocated
       * by the caller as well as its content
       */
      virtual std::vector<castor::stager::GCLocalFile*>*
      selectFiles2Delete(std::string diskServer)
        throw (castor::exception::Exception);

      /**
       * Informs the stager of files effectively deleted.
       * The DiskCopy id is given. The corresponding
       * DiskCopies will be deleted from the catalog
       * as well as the CastorFile if there is no other
       * copy.
       * @param diskCopyIds the list of diskcopies deleted
       * given by their id
       */
      virtual void filesDeleted
      (std::vector<u_signed64*>& diskCopyIds)
        throw (castor::exception::Exception);

      /**
       * Informs the stager of files for which deletion failed.
       * The DiskCopy id is given. The corresponding
       * DiskCopies will markes FAILED in the catalog.
       * @param diskCopyIds the list of diskcopies for which
       * deletion failed given by their id
       */
      virtual void filesDeletionFailed
      (std::vector<u_signed64*>& diskCopyIds)
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) was finished successfully.
       * The SubRequest and potentially the corresponding
       * Request will thus be removed from the DataBase
       * @param subReqId the id of the finished SubRequest
       */
      virtual void getUpdateDone(u_signed64 subReqId)
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Get or Update SubRequest
       * (without write) failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       */
      virtual void getUpdateFailed(u_signed64 subReqId)
        throw (castor::exception::Exception);

      /**
       * Informs the stager the a Put SubRequest failed.
       * The SubRequest's status will thus be set to FAILED
       * @param subReqId the id of the failing SubRequest
       */
      virtual void putFailed(u_signed64 subReqId)
        throw (castor::exception::Exception);

      /*
       * Get an array of segments that are in SEGMENT_FAILED
       * status. This method does not take any lock on the segments
       * and thus may return twice the same segments in two
       * different threads calling it at the same time
       * @return vector with all failed segments
       * @exception in case of error
       */
      virtual std::vector<castor::stager::Segment*>
      failedSegments()
        throw (castor::exception::Exception);

    protected:
      
      virtual int getRemoteStagerClientTimeout();


    }; // end of class RemoteStagerSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_REMOTESTAGERSVC_HPP
