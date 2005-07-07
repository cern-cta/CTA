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
 * @(#)$RCSfile: OraJobSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 15:08:21 $ $Author: itglp $
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
#include "castor/db/ora/OraCnvSvc.hpp"
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
        virtual inline const unsigned int id() const;

        /**
         * Get the service id
         */
        static const unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

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
         * Handles the start of a PutDone job.
         * Actually only returns the DiskCopy associated to the SubRequest
         * Note that deallocation of the DiskCopy is the
         * responsability of the caller.
         * If a null  DiskCopy is returned then the PutDone
         * was put in waiting mode and the caller should stop
         * processing it
         * @param subreqId the if of the SubRequest to consider
         * @return the DiskCopy to use for the data access or
         * 0 if the PutDone should wait
         * @exception Exception in case of error
         */
        virtual castor::stager::DiskCopy* putDoneStart(u_signed64 subreqId)
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
         * @exception Exception throws an Exception in case of error
         */
        virtual void prepareForMigration
        (castor::stager::SubRequest* subreq,
         u_signed64 fileSize)
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

      private:

	   /**
         * creation of TapeCopy(ies) and Segment(s) for
         * recall of a CastorFile. This methods calls the
         * name server and vmgr to gather information on
         * what needs to be recalled. It then creates the
         * appropriate TapeCopy(ies) and Segment(s) in the
         * database.
         * @param castorFile the CastorFile we want to recall
         * @param euid the euid of the user recalling the file
         * @param egid the egid of the user recalling the file
         * @exception Exception throws an Exception in case of error
         */
        void createTapeCopySegmentsForRecall
        (castor::stager::CastorFile *castorFile,
         unsigned long euid,
         unsigned long egid)
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

        /// SQL statement for function disk2DiskCopyDone
        static const std::string s_disk2DiskCopyDoneStatementString;

        /// SQL statement object for function disk2DiskCopyDone
        oracle::occi::Statement *m_disk2DiskCopyDoneStatement;

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

      }; // end of class OraJobSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAJOBSVC_HPP
