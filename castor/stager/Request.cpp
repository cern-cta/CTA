/******************************************************************************
 *                      castor/stager/Request.cpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IClient.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::Request::Request() throw() :
  m_flags(),
  m_userName(""),
  m_euid(0),
  m_egid(0),
  m_mask(0),
  m_pid(0),
  m_machine(""),
  m_svcClassName(""),
  m_userTag(""),
  m_svcClass(0),
  m_client(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::Request::~Request() throw() {
  for (unsigned int i = 0; i < m_subRequestsVector.size(); i++) {
    m_subRequestsVector[i]->setRequest(0);
    delete m_subRequestsVector[i];
  }
  m_subRequestsVector.clear();
  if (0 != m_client) {
    m_client->setRequest(0);
    delete m_client;
    m_client = 0;
  }
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Request::print(std::ostream& stream,
                                    std::string indent,
                                    castor::ObjectSet& alreadyPrinted) const {
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "flags : " << m_flags << std::endl;
  stream << indent << "userName : " << m_userName << std::endl;
  stream << indent << "euid : " << m_euid << std::endl;
  stream << indent << "egid : " << m_egid << std::endl;
  stream << indent << "mask : " << m_mask << std::endl;
  stream << indent << "pid : " << m_pid << std::endl;
  stream << indent << "machine : " << m_machine << std::endl;
  stream << indent << "svcClassName : " << m_svcClassName << std::endl;
  stream << indent << "userTag : " << m_userTag << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "SvcClass : " << std::endl;
  if (0 != m_svcClass) {
    m_svcClass->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  {
    stream << indent << "SubRequests : " << std::endl;
    int i;
    std::vector<SubRequest*>::const_iterator it;
    for (it = m_subRequestsVector.begin(), i = 0;
         it != m_subRequestsVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
  stream << indent << "Client : " << std::endl;
  if (0 != m_client) {
    m_client->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Request::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

