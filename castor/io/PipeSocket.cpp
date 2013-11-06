/******************************************************************************
 *                     PipeSocket.cpp
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
 *
 * A dedicated socket on top of standard file descriptors to be used
 * as communication channel between a parent and its forked children process
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <serrno.h>
#include <sys/types.h>
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/PipeSocket.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::PipeSocket::PipeSocket()
  throw (castor::exception::Exception) :
  AbstractSocket(0, false), m_mode(castor::io::PIPE_RW) 
{
  int fds[2];
  if(pipe(fds) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Failed to create a pipe";
    throw ex;
  }
  else {
    m_fdIn = fds[0];
    m_fdOut = fds[1];
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::PipeSocket::PipeSocket(const int fdIn, const int fdOut, const int mode)
  throw (castor::exception::Exception) :
  AbstractSocket(0, false), m_fdIn(fdIn), m_fdOut(fdOut), m_mode(mode) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::PipeSocket::~PipeSocket() throw()
{
  closeRead();
  closeWrite();
}

//------------------------------------------------------------------------------
// closeWrite
//------------------------------------------------------------------------------
void castor::io::PipeSocket::closeWrite()
{
  if((m_mode & castor::io::PIPE_WRITE) != 0) {
    ::close(m_fdOut);
    m_mode &= ~castor::io::PIPE_WRITE;
  }
}

//------------------------------------------------------------------------------
// closeRead
//------------------------------------------------------------------------------
void castor::io::PipeSocket::closeRead()
{
  if((m_mode & castor::io::PIPE_READ) != 0) {
    ::close(m_fdIn);
    m_mode &= ~castor::io::PIPE_READ;
  }
}

  
//------------------------------------------------------------------------------
// sendBuffer
//------------------------------------------------------------------------------
void castor::io::PipeSocket::sendBuffer(const unsigned int magic,
                                        const char* buf,
                                        const int n)
  throw (castor::exception::Exception) {
  if ((m_mode & castor::io::PIPE_WRITE) == 0) {
    castor::exception::Exception ex(0);
    ex.getMessage() << "Pipe closed for writing";
    throw ex;
  }
  // Sends the buffer with a header (magic number + size)
  unsigned int header[2];
  header[0] = magic;
  header[1] = (unsigned int)n;
  if (::write(m_fdOut,
            (char*)(&header),
            2 * sizeof(unsigned int)) != 2 * sizeof(unsigned int) ||
      ::write(m_fdOut, (char *)buf, n) != n) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to send data";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readBuffer
//------------------------------------------------------------------------------
void castor::io::PipeSocket::readBuffer(const unsigned int magic,
                                        char** buf,
                                        int& n)
  throw (castor::exception::Exception) {
  if ((m_mode & castor::io::PIPE_READ) == 0) {
    castor::exception::Exception ex(0);
    ex.getMessage() << "Pipe closed for reading";
    throw ex;
  }
  // First read the header
  unsigned int header[2];
  int ret = ::read(m_fdIn,
                   (char*)&header,
                   2*sizeof(unsigned int));
  if (ret != 2*sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to receive header\n"
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(errno);
      ex.getMessage() << "Unable to receive header";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Received header is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }
  if (header[0] != magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Bad magic number : 0x" << std::hex
                    << header[0] << " instead of 0x"
                    << std::hex << magic;
    throw ex;
  }
  // Now read the data
  n = header[1];
  *buf = (char*) malloc(n);
  ssize_t readBytes = 0;
  while (readBytes < n) {
    ssize_t nb = ::read(m_fdIn, (*buf)+readBytes, n - readBytes);
    if (nb == -1) {
      if (errno == EAGAIN) {
        continue;
      } else {
        break;
      }
    }
    readBytes += nb;
  }
  if (readBytes < n) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to receive all data. "
                    << "Got " << readBytes << " bytes instead of "
                    << n << ".";
    throw ex;
  }
}
