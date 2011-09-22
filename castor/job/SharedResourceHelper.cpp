/******************************************************************************
 *                      SharedResourceHelper.cpp
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
 * @(#)$RCSfile: SharedResourceHelper.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/07/29 06:22:21 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/job/SharedResourceHelper.hpp"
#include <errno.h>
#include <stdio.h>
#include <istream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include "Castor_limits.h"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::job::SharedResourceHelper::SharedResourceHelper() throw():
  m_url(""), m_content("") {}

//-----------------------------------------------------------------------------
// Download
//-----------------------------------------------------------------------------
std::string castor::job::SharedResourceHelper::download() 
  throw(castor::exception::Exception) {

  // File request from a shared filesystem ?
  if (m_url.substr(0, 7) == "file://") {
    
    // Stat the file to make sure it exists and is a regular file
    struct stat statbuf;
    if (stat(m_url.substr(7).c_str(), &statbuf) < 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << strerror(errno) << std::endl;
      throw e;
    } else if ((statbuf.st_mode & S_IFMT) != S_IFREG) {
      castor::exception::Exception e(EPERM);
      e.getMessage() << "Filepath does not point to a regular file" << std::endl;
      throw e;
    }
    
    // Open the file. We use fopen here as opposed to ifstream so that we can
    // have the value of errno to indicate why the open failed. ifstream for
    // example won't tell use about EACCESS errors.
    FILE *fp = fopen(m_url.substr(7).c_str(), "r");
    if (fp == NULL) {
      castor::exception::Exception e(errno);
      e.getMessage() << strerror(errno) << std::endl;
      throw e;
    } 
    // Read in the first line only!
    char buf[CA_MAXLINELEN + 1];
    if (fgets(buf, sizeof(buf), fp) != NULL) {
      m_content = buf;
    }
    fclose(fp);   
  }

  // Unknown URI
  else {
    castor::exception::Exception e(EINVAL);
    throw e;
  }

  return m_content;
}
