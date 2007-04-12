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

#ifndef MONITORING_CLUSTERSTATUS_HPP
#define MONITORING_CLUSTERSTATUS_HPP 1

#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/monitoring/SharedMemoryString.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"
#include <map>

namespace castor {

  namespace monitoring {

    /*
     * Describes the status of a cluster
     * Enforces usage of an external memory allocator for the DiskServer list
     */
    class ClusterStatus :
      public std::map<castor::monitoring::SharedMemoryString,
                      castor::monitoring::DiskServerStatus,
                      std::less<castor::monitoring::SharedMemoryString>,
                      castor::sharedMemory::Allocator
                      <std::pair<castor::monitoring::SharedMemoryString,
                                 castor::monitoring::DiskServerStatus>,
                       castor::monitoring::ClusterStatusBlockKey> > {

    public:

      /**
       * gets a pointer to the cluster status. This status
       * is a "shared memory singleton" meaning that this
       * method will return a pointer to the same instance
       * of ClusterStatus for all calls in all processes.
       * @param create when true, the shared memory singleton
       * is created if not found. The returned value tells
       * whether the singleton has been created or not.
       */
      static ClusterStatus* getClusterStatus(bool& create);

    public:

      /**
       * print method
       * @param out the stream where to print
       * @param indentation indentation prefixing each line
       */
      void print(std::ostream& out,
                 const std::string& indentation) const
        throw();

    }; // end ClusterStatus

  } // end monitoring

} // end castor

#endif // MONITORING_CLUSTERSTATUS_HPP
