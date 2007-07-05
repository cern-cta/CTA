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
 * @(#)PipeSocket.cpp,v 1.6 $Release$ 2006/01/17 09:52:22 itglp
 *
 * A dedicated socket on top of standard file descriptors to be used
 * as communication channel between a parent and its forked children process
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <net.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <serrno.h>
#include <sys/types.h>
#include <sys/select.h>
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/PipeSocket.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::PipeSocket::PipeSocket(const int fdIn, const int fdOut)
  throw (castor::exception::Exception) :
  AbstractSocket(0, false), m_fdIn(fdIn), m_fdOut(fdOut) {}

//------------------------------------------------------------------------------
// sendBuffer
//------------------------------------------------------------------------------
void castor::io::PipeSocket::sendBuffer(const unsigned int magic,
                                       const char* buf,
                                       const int n)
  throw (castor::exception::Exception) {
  // Sends the buffer with a header (magic number + size)
  if (::write(m_fdOut,
            (char*)(&magic),
            sizeof(unsigned int)) != sizeof(unsigned int) ||
      ::write(m_fdOut,
            (char*)(&n),
            sizeof(unsigned int)) != sizeof(unsigned int) ||
      ::write(m_fdOut, (char *)buf, n) != n) {
    castor::exception::Exception ex(serrno);
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
      castor::exception::Exception ex(serrno);
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
    ex.getMessage() << "Bad magic number : " << std::ios::hex
                    << header[0] << " instead of "
                    << std::ios::hex << magic;
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
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to receive all data. "
                    << "Got " << readBytes << " bytes instead of "
                    << n << ".";
    throw ex;
  }
}
