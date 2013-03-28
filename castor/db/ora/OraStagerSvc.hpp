/******************************************************************************
 *                castor/db/ora/OraStagerSvc.hpp
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
 * Implementation of the IStagerSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORASTAGERSVC_HPP
#define ORA_ORASTAGERSVC_HPP 1

// Include Files
#include "occi.h"

#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraCommonSvc.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include <vector>


namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IStagerSvc for Oracle
       */
      class OraStagerSvc : public OraCommonSvc,
                           public virtual castor::stager::IStagerSvc {

      public:

        /**
         * default constructor
         */
        OraStagerSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraStagerSvc() throw();

        /**
         * Get the service id
         */
        virtual inline unsigned int id() const;

        /**
         * Get the service id
         */
        static unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

      public:

        /**
         * Selects the next SubRequest the stager should deal with.
         * Selects a SubRequest in START, RESTART or RETRY status
         * and move its status to SUBREQUEST_WAITSCHED to avoid
         * double processing.
         * The selection is restricted to SubRequest associated to
         * requests of a given set of types, identified by service.
         * @param service to identify the group of requests to be queried
         * @return the SubRequest to process
         * @exception Exception in case of error
         */
        virtual castor::stager::SubRequest* subRequestToDo
        (const std::string service)
          throw (castor::exception::Exception);

        /**
         * Selects and processes a bulk request.
         * @param service identifies the group of bulk requests targetted
         * @return the result of the processing
         * @exception Exception in case of error
         */
        virtual castor::IObject* processBulkRequest
        (const std::string service)
          throw (castor::exception::Exception);

        /**
         * Selects the next SubRequest in FAILED status the stager
         * should deal with.
         * Selects a SubRequest in FAILED status and move its status
         * to FAILED_ANSWERING to avoid double processing.
         * @return the SubRequest to process
         * @exception Exception in case of system error
         */
        virtual castor::stager::SubRequest* subRequestFailedToDo()
          throw (castor::exception::Exception);

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
         * a DiskCopy was found and the SubRequest can be scheduled.
         * DISKCOPY_WAITDISK2DISKCOPY (1): like above, plus the
         * stager is allowed to replicate according to policies.
         * DISKCOPY_WAITTAPERECALL (2): no scheduling, no DiskCopy
         * found anywhere, a tape recall is needed.
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
          throw (castor::exception::Exception);

        /**
         * Processes a PToGet, PToUpdate subrequest.
         * @param subreq the SubRequest to consider
         * @return -2,-1,0,1,2, cf. return value for getDiskCopiesForJob.
         * @exception Exception in case of system error
         */
        virtual int processPrepareRequest
        (castor::stager::SubRequest* subreq)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
         * no MigrationJob of the given file is in MIGRATIONJOB_SELECTED
         * status and no DiskCopy of the file is in either
         * WAITFS, WAITFS_SCHEDULING, WAITTAPERECALL or
         * WAITDISK2DISKCOPY status. When recreation is not
         * possible, a null pointer is returned, with the exception
         * of WAITFS_SCHEDULING, where a DiskCopy is still returned
         * for logging purposes.
         * Else, all DiskCopies for the given file are marked
         * INVALID (that is those not in DISKCOPY_FAILED
         *status) and all Recall and MigrationJobs are
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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

        /*
         * cleanups the stager DB in case of a stagerRm for a renamed file
         * Handles accordingly the subrequest (failing it if needed)
         */
        virtual void renamedFileCleanup
        (const std::string &fileName, const u_signed64 subReqId)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

        /**
         * Triggers the recall of a file
         * @param srId the id of the subRequest triggering the recall
         * @return the status of the subrequest after the call (WAITTAPERECALL OR FAILED)
         * @exception Exception in case of error
         */
        virtual castor::stager::SubRequestStatusCodes createRecallCandidate(u_signed64 srId)
          throw (castor::exception::Exception);

        /**
         * Gets a configuration option from the CastorConfig table
         * @param class a string containing the option class (e.g. stager)
         * @param key a string containing the option key to be accessed
         * @param defaultValue a default return value when the key
         * is not found in the db
         * @return the option value
         * @exception in case of an error
         */
        virtual std::string getConfigOption(std::string confClass,
                                            std::string confKey,
                                            std::string defaultValue)
          throw (castor::exception::Exception);

        /**
         * Dumps the current logs from the db.
         * The content is logged in DLF and then deleted.
         */
        virtual void dumpDBLogs()
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function subRequestToDo
        static const std::string s_subRequestToDoStatementString;

        /// SQL statement object for function subRequestToDo
        oracle::occi::Statement *m_subRequestToDoStatement;

        /// SQL statement for function processBulkRequest
        static const std::string s_processBulkRequestStatementString;

        /// SQL statement object for function processBulkRequest
        oracle::occi::Statement *m_processBulkRequestStatement;

        /// SQL statement for function subRequestFailedToDo
        static const std::string s_subRequestFailedToDoStatementString;

        /// SQL statement object for function subRequestFailedToDo
        oracle::occi::Statement *m_subRequestFailedToDoStatement;

        /// SQL statement for function getDiskCopiesForJob
        static const std::string s_getDiskCopiesForJobStatementString;

        /// SQL statement object for function getDiskCopiesForJob
        oracle::occi::Statement *m_getDiskCopiesForJobStatement;

        /// SQL statement for function processPrepareRequest
        static const std::string s_processPrepareRequestStatementString;

        /// SQL statement object for function processPrepareRequest
        oracle::occi::Statement *m_processPrepareRequestStatement;

        /// SQL statement for function processPutDoneRequest
        static const std::string s_processPutDoneRequestStatementString;

        /// SQL statement object for function processPutDoneRequest
        oracle::occi::Statement *m_processPutDoneRequestStatement;

        /// SQL statement for function createDiskCopyReplicaRequest
        static const std::string s_createDiskCopyReplicaRequestStatementString;

        /// SQL statement object for function createDiskCopyReplicaRequest
        oracle::occi::Statement *m_createDiskCopyReplicaRequestStatement;

        /// SQL statement for function createEmptyFile
        static const std::string s_createEmptyFileStatementString;

        /// SQL statement object for function createEmptyFile
        oracle::occi::Statement *m_createEmptyFileStatement;

        /// SQL statement for function createRecallCandidate
        static const std::string s_createRecallCandidateStatementString;

        /// SQL statement object for function createRecallCandidate
        oracle::occi::Statement *m_createRecallCandidateStatement;

        /// SQL statement for function selectCastorFile
        static const std::string s_selectCastorFileStatementString;

        /// SQL statement object for function selectCastorFile
        oracle::occi::Statement *m_selectCastorFileStatement;

        /// SQL statement for function getBestDiskCopyToRead
        static const std::string s_getBestDiskCopyToReadStatementString;

        /// SQL statement object for function getBestDiskCopyToRead
        oracle::occi::Statement *m_getBestDiskCopyToReadStatement;

        /// SQL statement for function updateAndCheckSubRequest
        static const std::string s_updateAndCheckSubRequestStatementString;

        /// SQL statement object for function updateAndCheckSubRequest
        oracle::occi::Statement *m_updateAndCheckSubRequestStatement;

        /// SQL statement for function recreateCastorFile
        static const std::string s_recreateCastorFileStatementString;

        /// SQL statement object for function recreateCastorFile
        oracle::occi::Statement *m_recreateCastorFileStatement;

        /// SQL statement for function archiveSubReq
        static const std::string s_archiveSubReqStatementString;

        /// SQL statement object for function archiveSubReq
        oracle::occi::Statement *m_archiveSubReqStatement;

        /// SQL statement for function stageRm
        static const std::string s_stageRmStatementString;

        /// SQL statement object for function stageRm
        oracle::occi::Statement *m_stageRmStatement;

        /// SQL statement object for function renamedFileCleanup
        oracle::occi::Statement *m_renamedFileCleanupStatement;

        /// SQL statement for function setFileGCWeight
        static const std::string s_setFileGCWeightStatementString;

        /// SQL statement object for function setFileGCWeight
        oracle::occi::Statement *m_setFileGCWeightStatement;

        /// SQL statement to selectTapePriority
        static const std::string s_selectPriorityStatementString;

        /// SQL statement object for function selectPriority
        oracle::occi::Statement *m_selectPriorityStatement;

        /// SQL statement to enterTapePriority
        static const std::string s_enterPriorityStatementString;

        /// SQL statement object for function enterPriority
        oracle::occi::Statement *m_enterPriorityStatement;

        /// SQL statement to deleteTapePriority
        static const std::string s_deletePriorityStatementString;

        /// SQL statement object for function deletePriority
        oracle::occi::Statement *m_deletePriorityStatement;

        /// SQL statement for function getConfigOption
        static const std::string s_getConfigOptionStatementString;

        /// SQL statement object for function getConfigOption
        oracle::occi::Statement *m_getConfigOptionStatement;

        /// SQL statement for dumpDBLogs function
        static const std::string s_dumpDBLogsString;

        /// SQL statement object for dumpDBLogs function
        oracle::occi::Statement *m_dumpDBLogsStatement;

      }; // end of class OraStagerSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORASTAGERSVC_HPP
