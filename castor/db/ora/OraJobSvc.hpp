/******************************************************************************
 *                castor/db/ora/OraJobSvc.hpp
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
 * Implementation of the IJobSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAJOBSVC_HPP
#define ORA_ORAJOBSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraCommonSvc.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IJobSvc for Oracle
       */
      class OraJobSvc : public OraCommonSvc,
                        public virtual castor::stager::IJobSvc {

      public:

        /**
         * default constructor
         */
        OraJobSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraJobSvc() throw();

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
         * Handles the start of a Get or Update job.
         * @param subreq the SubRequest to consider
         * @param diskServerName the selected diskServer
         * @param mountPoint the mounPoint of the selected fileSystem
         * @param emptyFile whether the resulting diskCopy
         * deals with the recall of an empty file
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @return the DiskCopyPath to use for the data access or
         * a null pointer if the data access will have to wait
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
          throw (castor::exception::Exception);

        /**
         * Handles the start of a disk to disk copy. It checks
         * that the source DiskCopy stills exists i.e. hasn't been
         * garbage collected. Updates the filesystem of the destination
         * DiskCoy and verifies that the selected destination diskserver
         * and filesystem are valid for the given service class.
         * @param diskcopyId the id of the new DiskCopy
         * @param sourceDiskCopyId the id of the source diskCopy
         * @param diskServer the name of the destination diskserver
         * @param fileSystem the file system mount point
         * Changes are commited
         * @return diskCopy information about the destination DiskCopy
         * @return sourceDiskCopy information about the source DiskCopy
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @param euid the uid of the user issuing the command
         * @param egid the gid of the user issuing the command
         * @exception Exception in case of error
         */
        virtual void disk2DiskCopyStart
        (const u_signed64 diskCopyId,
         const u_signed64 sourceDiskCopyId,
         const std::string diskServer,
         const std::string fileSystem,
         castor::stager::DiskCopyInfo* &diskCopy,
         castor::stager::DiskCopyInfo* &sourceDiskCopy,
         u_signed64 fileId,
         const std::string nsHost)
          throw(castor::exception::Exception);

        /**
         * Updates database after successful completion of a
         * disk to disk copy. This includes setting the DiskCopy
         * status to status and setting the SubRequest
         * status to SUBREQUEST_READY.
         * Changes are commited
         * @param diskcopyId the id of the new DiskCopy
         * @param sourceDiskCopyId the id of the source diskCopy
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @param replicaFileSize the size of the newly replicated file
         * @exception Exception in case of error
         */
        virtual void disk2DiskCopyDone
        (u_signed64 diskCopyId,
         u_signed64 sourceDiskCopyId,
         u_signed64 fileId,
         const std::string nsHost,
         u_signed64 replicaFileSize)
          throw (castor::exception::Exception);

        /**
         * Updates database after a failure of a disk to disk copy.
         * Changes are commited
         * @param diskcopyId the id of the failed DiskCopy
         * @param enoent flag set to 1 when the sourceDiskCopy did
         * not exist, meaning that the process failed permanently
         * @param fileId the fileId of the CastorFile
         * @param nsHost the name server to use
         * @exception Exception throws an Exception in case of error
         */
        virtual void disk2DiskCopyFailed
        (u_signed64 diskCopyId,
         bool enoent,
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
         * @param subReqId the id of the SubRequest
         * @param fileSize The actual size of the castor file
         * @param timeStamp To know if the fileSize is still valid
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @param csumtype the checksum type of the castor file
         * @param csumvalue the checksum value of the castor file
         * @exception Exception in case of error
         */

        virtual void prepareForMigration
        (u_signed64 subReqId,
         u_signed64 fileSize, u_signed64 timeStamp,
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
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @exception Exception in case of error
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
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @exception Exception in case of error
         */
        virtual void getUpdateFailed
        (u_signed64 subReqId,
         u_signed64 fileId,
         const std::string nsHost)
          throw (castor::exception::Exception);

        /**
         * Informs the stager the a Put SubRequest failed.
         * The SubRequest's status will thus be set to FAILED
         * @param subReqId the id of the failing SubRequest
         * @param fileId the id of the castorFile
         * @param nsHost the name server hosting this castorFile
         * @exception Exception in case of error
         */
        virtual void putFailed
        (u_signed64 subReqId,
         u_signed64 fileId,
         const std::string nsHost)
          throw (castor::exception::Exception);

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
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function getUpdateStart
        static const std::string s_getUpdateStartStatementString;

        /// SQL statement object for function getUpdateStart
        oracle::occi::Statement *m_getUpdateStartStatement;

        /// SQL statement for function putStart
        static const std::string s_putStartStatementString;

        /// SQL statement object for function putStart
        oracle::occi::Statement *m_putStartStatement;

        /// SQL statement for function putDoneStart
        static const std::string s_putDoneStartStatementString;

        /// SQL statement object for function putDoneStart
        oracle::occi::Statement *m_putDoneStartStatement;

        /// SQL statement for function disk2DiskCopyStart
        static const std::string s_disk2DiskCopyStartStatementString;

        /// SQL statement object for function disk2DiskCopyStart
        oracle::occi::Statement *m_disk2DiskCopyStartStatement;

        /// SQL statement for function disk2DiskCopyDone
        static const std::string s_disk2DiskCopyDoneStatementString;

        /// SQL statement object for function disk2DiskCopyDone
        oracle::occi::Statement *m_disk2DiskCopyDoneStatement;

        /// SQL statement for function disk2DiskCopyFailed
        static const std::string s_disk2DiskCopyFailedStatementString;

        /// SQL statement object for function disk2DiskCopyFailed
        oracle::occi::Statement *m_disk2DiskCopyFailedStatement;

        /// SQL statement for function prepareForMigration
        static const std::string s_prepareForMigrationStatementString;

        /// SQL statement object for function prepareForMigration
        oracle::occi::Statement *m_prepareForMigrationStatement;

        /// SQL statement for function getUpdateDone
        static const std::string s_getUpdateDoneStatementString;

        /// SQL statement object for function getUpdateDone
        oracle::occi::Statement *m_getUpdateDoneStatement;

        /// SQL statement for function getUpdateFailed
        static const std::string s_getUpdateFailedStatementString;

        /// SQL statement object for function getUpdateFailed
        oracle::occi::Statement *m_getUpdateFailedStatement;

        /// SQL statement for function putFailed
        static const std::string s_putFailedStatementString;

        /// SQL statement object for function putFailed
        oracle::occi::Statement *m_putFailedStatement;

        /// SQL statement for function firstByteWritten
        static const std::string s_firstByteWrittenStatementString;

        /// SQL statement object for function firstByteWritten
        oracle::occi::Statement *m_firstByteWrittenStatement;

      }; // end of class OraJobSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAJOBSVC_HPP
