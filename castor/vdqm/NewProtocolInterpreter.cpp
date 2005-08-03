/******************************************************************************
 *                      NewProtocolInterpreter.cpp
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
 * @(#)RCSfile: NewProtocolInterpreter.cpp  Revision: 1.0  Release Date: Aug 3, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"

#include "castor/io/biniostream.h"
#include "castor/io/StreamAddress.hpp"
 
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
 
// Local includes
#include "NewProtocolInterpreter.hpp"
#include "VdqmServerSocket.hpp"
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::NewProtocolInterpreter::NewProtocolInterpreter(
	VdqmServerSocket* serverSocket,
	const Cuuid_t* cuuid) throw(castor::exception::Exception) {
	
	if ( 0 == serverSocket || 0 == cuuid) {
		castor::exception::InvalidArgument ex;
  	ex.getMessage() << "One of the arguments is NULL";
  	throw ex;
	}
	else {
		ptr_serverSocket = serverSocket;
		m_cuuid = cuuid;
	}
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::NewProtocolInterpreter::~NewProtocolInterpreter() 
	throw() {
}


//------------------------------------------------------------------------------
// readObject
//------------------------------------------------------------------------------
castor::IObject* castor::vdqm::NewProtocolInterpreter::readObject()
  throw(castor::exception::Exception) {

  char* buffer;
  int length;

  // reads from the socket
  readRestOfBuffer(&buffer, length);
  // package the buffer
  std::string sbuffer(buffer, length);
  castor::io::biniostream input(sbuffer);
  // unmarshalls the object
  castor::io::StreamAddress ad(input, "StreamCnvSvc", castor::SVC_STREAMCNV);
  
  castor::IObject* obj = svcs()->createObj(&ad);
  free(buffer);
  
  // return
  return obj;
}


//------------------------------------------------------------------------------
// readRestOfBuffer
//------------------------------------------------------------------------------
void castor::vdqm::NewProtocolInterpreter::readRestOfBuffer(char** buf, int& n)
  throw (castor::exception::Exception) {
  	
  // First read the header
  unsigned int length;
  int ret = netread(ptr_serverSocket->socket(),
                    (char*)&length,
                    sizeof(unsigned int));
  if (ret != sizeof(unsigned int)) {
    if (0 == ret) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to receive length" << std::endl
                      << "The connection was closed by remote end";
      throw ex;
    } else if (-1 == ret) {
      castor::exception::Exception ex(serrno);
      ex.getMessage() << "Unable to receive length";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Received length is too short : only "
                      << ret << " bytes";
      throw ex;
    }
  }

  // Now read the data
  n = length;
  *buf = (char*) malloc(n);
  if (netread(ptr_serverSocket->socket(), *buf, n) != n) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "Unable to receive data";
    throw ex;
  }
}
