/******************************************************************************
 *                      RmGetNodes.cpp
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
 * gets and displays the list of nodes handles by RmMasterDaemon
 * using shared memory
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <iostream>
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/exception/Exception.hpp"
#include "Cgetopt.h"

void help(std::string programName) {
  std::cout << "Usage: " << programName
            << " [-h,--help] [-n,--node diskServerName]"
            << std::endl;
}

int main(int argc, char** argv) {
  try {
    // Parse command line
    Coptind = 1; /* Required */
    Coptions_t longopts[] = {
      {"help", NO_ARGUMENT, NULL, 'h'},
      {"node", REQUIRED_ARGUMENT, NULL, 'n'},
      {0, 0, 0, 0}
    };
    char c;
    char* node = 0;
    while ((c = Cgetopt_long(argc, argv, "hn:", longopts, NULL)) != -1) {
      switch (c) {
      case 'n':
        node = strdup(Coptarg);
        break;
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
      std::cerr << "This command takes no argument\n";
      help(argv[0]);
      exit(-1);
    }
    // Get shared memory object
    bool create = false;
    castor::monitoring::ClusterStatus* cs =
      castor::monitoring::ClusterStatus::getClusterStatus(create);
    if(cs == 0) {
      std::cout << "No shared memory area found. Please start RmMasterDaemon first."
                << std::endl << std::endl;
      return -1;
    }
    // Check whether we want to print everything or only a given machine
    if (0 == node) {
      cs->print(std::cout, "");
    } else {
      castor::monitoring::ClusterStatus::const_iterator it =
        cs->find(node);
      if (cs->end() != it) {
        std::cout << "\t\tname" << ": " << it->first << "\n";
        it->second.print(std::cout, "");
      } else {
        std::cerr << "No diskServer found with name '"
                  << node << "'. Maybe check the domain."
                  << std::endl;
      }
    }
  } catch (castor::exception::Exception e) {
    std::cout << e.getMessage().str() << std::endl;
  }
}
