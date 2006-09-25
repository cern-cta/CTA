/******************************************************************************
 *                      ClusterStatus.cpp
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

#include "castor/rmmaster/ClusterStatus.hpp"
#include "castor/sharedMemory/Block.hpp"
#include "castor/sharedMemory/Helper.hpp"

//------------------------------------------------------------------------------
// getClusterStatus
//------------------------------------------------------------------------------
castor::rmmaster::ClusterStatus*
castor::rmmaster::ClusterStatus::getClusterStatus() {
  // get the shared memory block
  castor::sharedMemory::Block* smBlock =
    castor::sharedMemory::Helper::getBlock();
  // return the Cluster Status
  return smBlock->clusterStatus();
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rmmaster::ClusterStatus::print(std::ostream& out) throw() {
  if (0 == size()) {
    out << "No machine in shared memory" << std::endl;
    return;
  }
  for (iterator it = this->begin(); it != this->end(); it++) {
    out << it->first;
    it->second.print(out);
  }
}
