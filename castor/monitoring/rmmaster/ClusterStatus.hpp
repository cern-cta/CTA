/******************************************************************************
 *                      ClusterStatus.hpp
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
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RMMASTER_CLUSTERSTATUS_HPP 
#define RMMASTER_CLUSTERSTATUS_HPP 1

#include "castor/rmmaster/MachineStatus.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/string"
#include <map>

namespace castor {

  namespace rmmaster {

    /*
     * Describes the status of a cluster
     * Enforces usage of an external memory allocator for the Machine list
     */
    class ClusterStatus :
      public std::map<castor::sharedMemory::string,
		      castor::rmmaster::MachineStatus,
                      std::less<castor::sharedMemory::string>,
		      castor::sharedMemory::Allocator
		      <std::pair<castor::sharedMemory::string,
				 castor::rmmaster::MachineStatus> > > {

    public:

      /**
       * gets a pointer to the cluster status. This status
       * is a "shared memory singleton" meaning that this
       * method will return a pointer to the same instance
       * of ClusterStatus for all calls in all processes
       */
      static ClusterStatus* getClusterStatus();

    public:

      /**
       * print method
       * @param out the stream where to print
       */
      void print(std::ostream& out) throw();

    }; // end ClusterStatus

  } // end rmmaster

} // end castor

#endif // RMMASTER_CLUSTERSTATUS_HPP
