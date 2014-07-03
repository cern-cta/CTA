/******************************************************************************
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

#pragma once

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/stager/IGCSvc.hpp"
#include <vector>

namespace castor {

  namespace stager {

    /**
     * Constants for the configuration variables
     */
    extern const char* RMTGCSVC_CATEGORY_CONF;
    extern const char* TIMEOUT_CONF;
    extern const int   DEFAULT_REMOTEGCSVC_TIMEOUT;

    /**
     * Implementation of the Remote GC service
     */
    class RemoteGCSvc : public BaseSvc,
                        public virtual castor::stager::IGCSvc {

    public:

      /**
       * default constructor
       */
      RemoteGCSvc(const std::string name);

      /**
       * default destructor
       */
      virtual ~RemoteGCSvc() throw();

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
       * List files to be deleted on a given diskServer.
       * These are the files corresponding to DiskCopies in
       * STAGED status and eligible for garbage collection,
       * plus the INVALID ones. This status is changed
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
        ;

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
        ;

      /**
       * Informs the stager of files for which deletion failed.
       * The DiskCopy id is given. The corresponding
       * DiskCopies will markes FAILED in the catalog.
       * @param diskCopyIds the list of diskcopies for which
       * deletion failed given by their id
       */
      virtual void filesDeletionFailed
      (std::vector<u_signed64*>& diskCopyIds)
        ;

      /**
       * Handles a set of files that were deleted from
       * a nameServer but may still be in the stager.
       * Proper cleaning will be done both of the diskServers
       * where copies of these files exist and of the stager DB
       * @param fileIds the set of files, given by fileids
       * @param nsHost the nameserver in which they reside
       * @return the list of fileIds that were not found in the stager
       */
      virtual std::vector<u_signed64> nsFilesDeleted
      (std::vector<u_signed64> &fileIds,
       std::string nsHost) throw();

      /**
       * Find the files which are not anymore in the Stager
       * @param diskCopyIds a list of diskcopy ids to be checked
       * @param nsHost the nameserver in which they reside
       * @return the list of diskcopy ids that were not found in the stager
       */
      virtual std::vector<u_signed64> stgFilesDeleted
      (std::vector<u_signed64> &diskCopyIds,
       std::string nsHost) throw();

      /**
       * Dumps the current log table produced by the cleaning db job.
       * The content is logged in DLF and then deleted.
       */
      virtual void dumpCleanupLogs()
        ;

      /**
       * Removes requests older than a given timeout, taken
       * from the configuration table in the db
       */
      virtual void removeTerminatedRequests()
        ;


      /////// ICommonSvc part (not implemented)

      /**
       * Selects the next request the GC service should deal with.
       * Selects a Request in START status and move its status
       * PROCESSED to avoid double processing.
       * @return the Request to process
       * @exception Exception in case of error
       */
      virtual castor::stager::Request* requestToDo(std::string service)
        ;

    protected:

      virtual int getRemoteGCClientTimeout();


    }; // end of class RemoteGCSvc

  } // end of namespace stager

} // end of namespace castor

