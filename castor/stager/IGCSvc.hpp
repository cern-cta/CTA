/******************************************************************************
 *                castor/stager/IGCSvc.hpp
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
 * @(#)$RCSfile: IGCSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 14:58:42 $ $Author: itglp $
 *
 * This class provides stager methods related to Garbage Collection
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_IGCSVC_HPP
#define STAGER_IGCSVC_HPP 1

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
    class GCLocalFile;

    /**
     * This class provides stager methods related to Garbage Collection
     */
    class IGCSvc : public virtual ICommonSvc {

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
        throw (castor::exception::Exception) = 0;

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
        throw (castor::exception::Exception) = 0;

      /**
       * Informs the stager of files for which deletion failed.
       * The DiskCopy id is given. The corresponding
       * DiskCopies will marked FAILED in the catalog.
       * @param diskCopyIds the list of diskcopies for which
       * deletion failed given by their id
       */
      virtual void filesDeletionFailed
      (std::vector<u_signed64*>& diskCopyIds)
        throw (castor::exception::Exception) = 0;

    }; // end of class IGCSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_IGCSVC_HPP
