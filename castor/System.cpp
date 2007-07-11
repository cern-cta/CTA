/******************************************************************************
 *                      System.cpp
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
 * @(#)BaseClient.cpp,v 1.37 $Release$ 2006/02/16 15:56:58 sponcec3
 *
 * A class with static methods for system level utilities.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <errno.h>

// Local includes
#include "System.hpp"

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string castor::System::getHostName() throw (castor::exception::Exception)
{ 
  // All this to get the hostname, thanks to C !
  int len = 64;
  char* hostname;
  hostname = (char*) calloc(len, 1);
  if (gethostname(hostname, len) < 0) {
    // Test whether error is due to a name too long
    // The errno depends on the glibc version
    if (EINVAL != errno &&
        ENAMETOOLONG != errno) {
      //clog() << "Unable to get hostname : "
      //       << strerror(errno) << std::endl;
      free(hostname);
      castor::exception::Exception e(errno);
      e.getMessage() << "gethostname error";
      throw e;

    }
    // So the name was too long
    while (hostname[len - 1] != 0) {
      len *= 2;
      char *hostnameLonger = (char*) realloc(hostname, len);
      if (0 == hostnameLonger) {
        //clog() << "Unable to allocate memory for hostname."
        //       << std::endl;
        free(hostname);
        castor::exception::Exception e(ENOMEM);
        e.getMessage() << "Could not allocate memory for hostname";
        throw e;

      }
      hostname = hostnameLonger;
      memset(hostname, 0, len);
      if (gethostname(hostname, len) < 0) {
        // Test whether error is due to a name too long
        // The errno depends on the glibc version
        if (EINVAL != errno &&
            ENAMETOOLONG != errno) {
          //clog() << "Unable to get hostname : "
          //       << strerror(errno) << std::endl;
          free(hostname);
          castor::exception::Exception e(errno);
          e.getMessage() << "Could not get hostname"
                         <<  strerror(errno);
          throw e;
        }
      }
    }
  }
  std::string res(hostname);   // copy the string
  free(hostname);
  return res;
}
