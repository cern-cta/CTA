/******************************************************************************
 *                      DumpSharedMemory.cpp
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
 * @(#)$RCSfile: DumpSharedMemory.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/04/11 08:50:06 $ $Author: sponcec3 $
 *
 * this is a little executable able to dump the content of
 * the monitoring shared memory block for debugging purposes
 *
 * @author castor dev team
 *****************************************************************************/

#include "castor/sharedMemory/SingletonBlock.hpp"
#include "castor/sharedMemory/BlockDict.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"
#include <iostream>

int main(int argc, char** argv) {

  // get SharedMemoryBlock
  castor::sharedMemory::BlockKey key =
    castor::monitoring::ClusterStatusBlockKey::getBlockKey();
  castor::sharedMemory::SingletonBlock
    <castor::monitoring::ClusterStatus,
    castor::sharedMemory::Allocator
    <castor::sharedMemory::SharedNode,
    castor::monitoring::ClusterStatusBlockKey> > *b =
    castor::sharedMemory::BlockDict::getBlock
    <castor::sharedMemory::SingletonBlock
    <castor::monitoring::ClusterStatus,
    castor::sharedMemory::Allocator
    <castor::sharedMemory::SharedNode,
    castor::monitoring::ClusterStatusBlockKey> > >(key);

  // print content roughly
  b->print(std::cout, "");

  // print details of the Cluster status map
  castor::monitoring::ClusterStatus* smStatus =
    b->getSingleton();
  void** p = (void**)smStatus;
  // From now on, we assume gcc 3.2 stdc++ lib
  std::cout << "----------------------------------\n"
	    << "-- Details on ClusterStatus map --\n"
	    << "----------------------------------\n";
  std::cout << "allocator : " << std::hex << *p << "\n";
  p = (void**)(((char*)p) + sizeof(void*));
  std::cout << "header : " << std::hex << *p << "\n";
  p = (void**)(((char*)p) + sizeof(void*));
  std::cout << "node_count : " << (int)(*p) << "\n";
  p = (void**)(((char*)p) + sizeof(void*));
  std::cout << "key_compare : " << *p << std::endl;
  return 0;
}
