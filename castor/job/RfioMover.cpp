/******************************************************************************
 *                      RfioMover.cpp
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
 * @(#)$RCSfile: RfioMover.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/01/21 07:34:54 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/job/RfioMover.hpp"
#include "getconfent.h"
#include "rfio_api.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

// Defaults
#define RFIO_DEFAULT_BUFFER_SIZE (128 * 1024)


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::job::RfioMover::RfioMover() :
  m_outputFile(""),
  m_inputFile(""),
  m_outputFD(0),
  m_inputFD(0),
  m_totalBytes(0) {
  
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::job::RfioMover::~RfioMover() throw() {

  // Close the source file descriptor
  if (m_inputFD > 0) {
    rfio_close(m_inputFD);
    m_inputFD = -1;
  }
  
  // Close the destination file descriptor
  if (m_outputFD > 0) {
    rfio_close(m_outputFD);
    m_outputFD = -1;
  }
}


//-----------------------------------------------------------------------------
// Source
//-----------------------------------------------------------------------------
void castor::job::RfioMover::source()
  throw(castor::exception::Exception) {

}


//-----------------------------------------------------------------------------
// Destination
//-----------------------------------------------------------------------------
void castor::job::RfioMover::destination
(castor::stager::DiskCopyInfo *diskCopy,
 castor::stager::DiskCopyInfo *sourceDiskCopy)
  throw(castor::exception::Exception) {

  // Construct the input and output filepaths
  m_outputFile = diskCopy->mountPoint() + diskCopy->diskCopyPath();
  m_inputFile  = sourceDiskCopy->diskServer() + ":" 
    + sourceDiskCopy->mountPoint() + sourceDiskCopy->diskCopyPath();

  // Activate V3 RFIO protocol (Streaming mode)
  int v = RFIO_STREAM;
  rfiosetopt(RFIO_READOPT, &v, 4);
  
  // Before any RFIO calls it appears necessary to reset the rfio_errno and
  // serrno error indicators!! This is also true within the rfcp source code
  rfio_errno = serrno = 0;

  // Open the source file 
  m_inputFD = rfio_open64((char *)m_inputFile.c_str(), O_RDONLY, 0644);
  if (m_inputFD < 0) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to rfio_open64 the source: " 
		   << m_inputFile << " : " << rfio_serror() << std::endl;
    throw e;
  }  
  
  // Stat the source file
  struct stat64 statbuf;
  rfio_errno = serrno = 0;
  if (rfio_fstat64(m_inputFD, &statbuf) != 0) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to rfio_fstat64 the source: " 
		   << m_inputFile << " : " << rfio_serror() << std::endl;
    throw e;
  }

  // Set the file size of the source file
  m_fileSize = statbuf.st_size;

  // Activate V3 RFIO protocol again (???)
  rfiosetopt(RFIO_READOPT, &v, 4);
  
  // Open the destination file
  rfio_errno = serrno = 0;
  m_outputFD = rfio_open64((char *)m_outputFile.c_str(), 
			   O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
  if (m_outputFD < 0) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to rfio_open64 the destination: "
		   << m_outputFile << " : " << rfio_serror() << std::endl;
    throw e;
  }

  // Copy the file
  try {
    copyFile();
  } catch (castor::exception::Exception e) {
    throw e;
  }
}


//-----------------------------------------------------------------------------
// CleanupFile
//-----------------------------------------------------------------------------
void castor::job::RfioMover::cleanupFile(bool silent, bool unlink)
  throw(castor::exception::Exception) {

  // Close the source file
  rfio_errno = serrno = 0;
  if ((m_inputFD > 0) && (rfio_close(m_inputFD) < 0)) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to rfio_close the source: "
		   << rfio_serror() << std::endl;
    m_inputFD = -1;
    if (!silent) throw e;
  }
  m_inputFD = -1;

  // Close the destination file descriptor. If the close fails unlink the file
  // but only after checking its S_IFREG.
  struct stat64 statbuf;
  rfio_errno = serrno = 0;
  if ((m_outputFD > 0) && (rfio_close(m_outputFD) < 0)) {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Failed to rfio_close the destination: "
		   << rfio_serror() << " - unlinking file" << std::endl;
    rfio_errno = serrno = 0;
    if (rfio_stat64((char *)m_outputFile.c_str(), &statbuf) == 0) {
      if ((statbuf.st_mode & S_IFMT) == S_IFREG) {
	rfio_errno = serrno = 0;
       	rfio_unlink((char *)m_outputFile.c_str());
      }
    }
    m_outputFD = -1;
    if (!silent) throw e;
  }
  m_outputFD = -1;

  // File removal necessary?
  if (unlink) {
    rfio_errno = serrno = 0;
    if (rfio_stat64((char *)m_outputFile.c_str(), &statbuf) == 0) {
      if ((statbuf.st_mode & S_IFMT) == S_IFREG) {
	rfio_errno = serrno = 0;
       	rfio_unlink((char *)m_outputFile.c_str());
      }
    }
  }
}


//-----------------------------------------------------------------------------
// CopyFile
//-----------------------------------------------------------------------------
void castor::job::RfioMover::copyFile()
  throw(castor::exception::Exception) {

  // Get the RFCP buffer size
  char *value = getconfent("RFCP", "BUFFERSIZE", 0);
  int bufsize = RFIO_DEFAULT_BUFFER_SIZE;
  if (value) {
    bufsize = std::strtol(value, 0, 10);
    if (bufsize <= 0) {
      cleanupFile(true, true);
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "Invalid RFCP/BUFFERSIZE option, transfer aborted"
		     << std::endl;
      throw e;      
    }
  } 

  // Allocate memory for the buffer area where data from the source will be
  // read into.
  char *copyBuffer = (char *)malloc(bufsize);
  if (copyBuffer == NULL) {
    cleanupFile(true, true);
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to allocate space for copy buffer" << std::endl;
    throw e;
  }

  // Perform the copy operation
  int n = 0;
  do {
    // Check to see if a shutdown request has been made and abort the transfer
    // with deletion of the output file.
    if (m_shutdown) {
      cleanupFile(true, true);
      castor::exception::Exception e(EINTR);
      e.getMessage() << "Acknowledging shutdown request in mover" << std::endl;
      throw e;
    }

    // By definition totalsize is always positive
    if ((signed)(m_fileSize - m_totalBytes) < bufsize) {
      bufsize = m_fileSize - m_totalBytes;
    }

    // Read data from the source/destination
    rfio_errno = serrno = 0;
    n = rfio_read(m_inputFD, copyBuffer, bufsize);
    if (n > 0) {
      
      // Write data to disk
      int count = 0, m = 0;
      rfio_errno = serrno = 0;
      while ((count != n && 
	      (m = rfio_write(m_outputFD, copyBuffer + count, n - count)) > 0)) {
	count += m;
      }
      m_totalBytes += n;
      m_bytesTransferred += n;

      // Writing failed ?
      if (m < 0) {
	cleanupFile(true, true);
	castor::exception::Exception e(SEINTERNAL);
	e.getMessage() << "Failed to rfio_write on destination: "
		       << m_outputFile << " : " << rfio_serror() << std::endl;
	throw e;
      } 
    } else if (n < 0) {
      // Here we encountered a read error. We simply break and allow the
      // cleanup to run.
      break;
    }		  
  } while ((m_fileSize != m_totalBytes) && (n > 0));
  free(copyBuffer);

  // Attempt to close the file descriptors and deal with errors gracefully.
  // If no exception is thrown then the transfer is deemed successful.
  cleanupFile(false, false);
}
