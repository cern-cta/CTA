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
#include "castor/exception/Internal.hpp"
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
castor::job::SharedResourceHelper::SharedResourceHelper
(unsigned int retryAttempts, unsigned int retryInterval)
  throw(castor::exception::Exception) :
  m_curl(NULL),
  m_errorCode(CURLE_OK),
  m_retryAttempts(retryAttempts),
  m_retryInterval(retryInterval),
  m_url(""),
  m_content("") {

  // Initialize the environment that cURL needs. Multiple calls will have no
  // effect here
  curl_global_init(CURL_GLOBAL_ALL);

  // Initialize the cURL handle.
  m_curl = curl_easy_init();
  if (m_curl == NULL) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "curl_easy_init library call failure" << std::endl;
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::job::SharedResourceHelper::~SharedResourceHelper() throw() {
  if (m_curl != NULL) {
    curl_easy_cleanup(m_curl);
  }
  curl_global_cleanup();
}


//-----------------------------------------------------------------------------
// WriteMemoryCallback
//-----------------------------------------------------------------------------
static size_t WriteMemoryCallback
(void *ptr, size_t size, size_t nmemb, void *data) {
  std::string *buf = static_cast<std::string *>(data);
  *buf += reinterpret_cast<char *>(ptr);
  return size * nmemb;
}


//-----------------------------------------------------------------------------
// Download
//-----------------------------------------------------------------------------
std::string castor::job::SharedResourceHelper::download(bool debug) 
  throw(castor::exception::Exception) {

  // HTTP download required ?
  memset(m_errorBuffer, 0, CURL_ERROR_SIZE);
  if (m_url.substr(0, 7) == "http://") {
    
    // Set the cURL options
    curl_easy_setopt(m_curl, CURLOPT_URL, m_url.c_str());
    curl_easy_setopt(m_curl, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(m_curl, CURLOPT_HEADER, 0);
    curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, 600);
    curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(m_curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(m_curl, CURLOPT_ERRORBUFFER, m_errorBuffer);
    curl_easy_setopt(m_curl, CURLOPT_FAILONERROR, 1);
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &m_content);
    curl_easy_setopt(m_curl, CURLOPT_VERBOSE, debug);
    
    // Download the file. We use a retry mechanism here to be able to deal
    // with transient error situations from the remote http daemon such as
    // restarts or reboots.
    for (u_signed64 i = 0; i < m_retryAttempts; i++) {
      m_errorCode = curl_easy_perform(m_curl);
      if (m_errorCode == CURLE_OK) {
	break; // Success
      }
      
      // Maximum attempts exceeded
      else if ((i + 1) == m_retryAttempts) {
	castor::exception::Exception e(SERTYEXHAUST);
	throw e;
      }
      sleep(m_retryInterval);
    }
  }

  // File request from a shared filesystem ?
  else if (m_url.substr(0, 7) == "file://") {
    
    // We reset the maximum number of retry attempts for file based URI's as we
    // don't allow multiple retries.
    m_retryAttempts = 1;
    m_retryInterval = 0;

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


//-----------------------------------------------------------------------------
// SetUrl
//-----------------------------------------------------------------------------
void castor::job::SharedResourceHelper::setUrl(std::string url)
  throw (castor::exception::Exception) {

  // For file:// requests we do not perform any string subsitution of the
  // LSB_MASTERNAME variable
  if (url.substr(0, 7) == "file://") {
    m_url = url;
    return;
  }

  // Perform variable subsitution
  std::string::size_type index = url.find("LSB_MASTERNAME");
  if (index != std::string::npos) {
   
    // Initialize the LSF library
    if (lsb_init((char*)"SharedResourceHelper") < 0) {
      
      // "Failed to initialize the LSF batch library (LSBLIB)"
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Failed to initialize the LSF batch library (LSBLIB): "
		     << lsberrno ? lsb_sysmsg() : "no message";
      throw e;
    }

    // Fetch the LSF master name
    char *masterName = ls_getmastername();
    if (masterName == NULL) {

      // "Failed to determine the name of the current LSF master"
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Failed to determine the name of the current LSF master: "
		     << lsberrno ? lsb_sysmsg() : "no message";
      throw e;
    }
    m_url = url.substr(0, index) + masterName + url.substr(index + 14, url.length());
  } else {
    m_url = url;
  }
}
