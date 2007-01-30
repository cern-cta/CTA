/******************************************************************************
 *                      ClusterStatusBlockKey.cpp
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

#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"

#define SHARED_MEMORY_SIZE 1048576
#define SHARED_MEMORY_KEY 2374
#define SHARED_MEMORY_ADDRESS 0x30000000

//------------------------------------------------------------------------------
// getBlockKey
//------------------------------------------------------------------------------
castor::sharedMemory::BlockKey
castor::monitoring::ClusterStatusBlockKey::getBlockKey() throw() {
  castor::sharedMemory::BlockKey b(SHARED_MEMORY_KEY,
                                   SHARED_MEMORY_SIZE,
                                   (void*)SHARED_MEMORY_ADDRESS);
  return b;
}

#undef SHARED_MEMORY_SIZE
#undef SHARED_MEMORY_KEY
#undef SHARED_MEMORY_ADDRESS
