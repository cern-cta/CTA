/******************************************************************************
 *                      checkSharedMemory.cpp
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

#define private public
#include <iostream>
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/SingletonBlock.hpp"
#include "castor/sharedMemory/Block.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/exception/Exception.hpp"
#include "Cgetopt.h"

// couple of useful typedef, in this forest of templated types
typedef castor::sharedMemory::Allocator
<castor::sharedMemory::SharedNode,
 castor::monitoring::ClusterStatusBlockKey> Allocator;
typedef castor::sharedMemory::SingletonBlock
<castor::monitoring::ClusterStatus,
 Allocator> Block;
typedef std::map<void*, size_t, std::less<void*>, Allocator> SharedMap;

void help(std::string programName) {
  std::cout << "Usage: " << programName
            << " [-h,--help]"
            << std::endl;
}

int main(int argc, char** argv) {
  try {
    // Parse command line
    Coptind = 1; /* Required */
    Coptions_t longopts[] = {
      { "help", NO_ARGUMENT, NULL, 'h' },
      { 0, 0, 0, 0}
    };
    char c;
    while ((c = Cgetopt_long(argc, argv, "h", longopts, NULL)) != -1) {
      switch (c) {
      case 'h':
        help(argv[0]);
        exit(0);
      default:
	std::cerr << "Unknown option \n";
        help(argv[0]);
        exit(-1);
      }
    }
    argc -= Coptind;
    if (argc > 0) {
      help(argv[0]);
      exit(-1);
    }
    // Get shared memory block
    castor::sharedMemory::BlockKey key =
      castor::monitoring::ClusterStatusBlockKey::getBlockKey();
    bool create = false;
    Block *b = castor::sharedMemory::BlockDict::getBlock<Block>(key, create);
    if (0 == b) {
      std::cout << "No shared memory area found."
                << std::endl << std::endl;
      return -1;
    }
    // Find out free space
    unsigned int freeSpace = 0;
    for (SharedMap::iterator it =
           b->m_freeRegions->begin();
         it != b->m_freeRegions->end();
         it++) {
      freeSpace += it->second;
    }
    std::cout << "Free space in shared memory : " << freeSpace << " bytes" << std::endl;
    
    // Count diskservers and filesystems used
    castor::monitoring::ClusterStatus* cs = b->getSingleton();
    if (cs->begin() == cs->end()) {
      std::cout << "No diskservers registered" << std::endl;
      return 0;
    }

    // Loop over cluster
    unsigned int nbDS = cs->size();
    unsigned int nbFS = 0;
    unsigned int nbDeletedDS = 0;
    unsigned int nbDeletedFS = 0;
    for (castor::monitoring::ClusterStatus::const_iterator it =
	   cs->begin();
	 it != cs->end();
	 it++) {
      // Only display deleted diskservers if instructed too
      if (it->second.adminStatus() == castor::monitoring::ADMIN_DELETED) {
        nbDeletedDS++;
        nbDeletedFS += it->second.size();
      }
      nbFS += it->second.size();
    }
    std::cout << "Shared memory contains :\n"
              << nbDS << " diskServers and " << nbFS << " fileSystems\n"
              << "out of which " << nbDeletedDS << " diskServers and "
              << nbDeletedFS << " fileSystems are deleted" << std::endl;
    
  } catch (castor::exception::Exception e) {
    std::cout << e.getMessage().str() << std::endl;
  }
}
