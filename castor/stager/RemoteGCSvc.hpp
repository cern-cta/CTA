/******************************************************************************
 *                      RemoteGCSvc.hpp
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
 * @(#)$RCSfile: RemoteGCSvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2005/08/18 10:18:50 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_REMOTEGCSVC_HPP
#define STAGER_REMOTEGCSVC_HPP 1

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
      virtual inline const unsigned int id() const;

      /**
       * Get the service id
       */
      static const unsigned int ID();

    public:

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


      /////// ICommonSvc part (not implemented)

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
       * Retrieves a FileSystem from the database based on its
       * mount point and diskServer name. Keeps a lock on it.
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
       * Retrieves a tape from the database based on its vid,
       * side and tpmode. If no tape is found, creates one.
       * Note that this method creates a lock on the row of the
       * given tape and does not release it. It is the
       * responsability of the caller to commit the transaction.
       * The caller is also responsible for the deletion of the
       * allocated object
       * @param vid the vid of the tape
       * @param side the side of the tape
       * @param tpmode the tpmode of the tape
       * @return the tape. the return value can never be 0
       * @exception Exception in case of error (no tape found,
       * several tapes found, DB problem, etc...)
       */
      virtual castor::stager::Tape* selectTape(const std::string vid,
                                               const int side,
                                               const int tpmode)
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

    protected:
      
      virtual int getRemoteGCClientTimeout();


    }; // end of class RemoteGCSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_REMOTEGCSVC_HPP
