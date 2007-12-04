/******************************************************************************
 *                      DiskServerStatus.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Describes the status of one diskServer
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RMMASTER_DISKSERVERSTATUS_HPP
#define RMMASTER_DISKSERVERSTATUS_HPP 1

// Include files
#include "castor/rmmaster/FileSystemStatus.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/string"
#include <map>

namespace castor {

  namespace rmmaster {

    /*
     * Describes the status of one diskServer
     * Enforces usage of an external memory allocator for the Filesystem list
     */
    class DiskServerStatus :
      public std::map<castor::sharedMemory::string,
		      castor::rmmaster::FileSystemStatus,
                      std::less<castor::sharedMemory::string>,
		      castor::sharedMemory::Allocator
		      <std::pair<castor::sharedMemory::string,
				 castor::rmmaster::FileSystemStatus> > > {

    public:

      /*
       * Constructor
       */
      DiskServerStatus(u_signed64 id = 0);

      /**
       * print method
       */
      void print(std::ostream& out) throw();

    public:

      /// Accessor to diskServer id
      u_signed64 id() const { return m_id; }

    private:

      /// The diskServer id
      u_signed64 m_id;

    }; // end DiskServerStatus

  } // end rmmaster

} // end castor

#endif // RMMASTER_DISKSERVERSTATUS_HPP
