/******************************************************************************
 *                      StageIn.cpp
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
 * @(#)$RCSfile: StageIn.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/05/28 08:56:54 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <cstdlib>
#include "castor/ObjectSet.hpp"
#include "castor/rh/File.hpp"
#include "castor/rh/StageInRequest.hpp"
#include "castor/exception/Exception.hpp"

// Local Files
#include "StageIn.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::rh::Request* castor::client::StageIn::buildRequest()
  throw (castor::exception::Exception) {
  // set up Client
  if (m_inputFlags.find("-h") != m_inputFlags.end()) {
    m_rhHost = m_inputFlags["-h"];
    std::cout << m_rhHost << ":";
  } else {
    
  }
  if (m_inputFlags.find("-p") != m_inputFlags.end()) {
    m_rhPort = atoi(m_inputFlags["-p"].c_str());
    std::cout << m_rhPort << std::endl;
  } else {
    
  }
  // Build request
  castor::rh::StageInRequest* req =
    new castor::rh::StageInRequest();
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::rh::File* f = new castor::rh::File();
    f->setName(*it);
    req->addFiles(f);
    f->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// printResult
//------------------------------------------------------------------------------
void castor::client::StageIn::printResult(castor::IObject& result)
  throw (castor::exception::Exception) {
  castor::ObjectSet set;
  result.print(std::cout, "", set);
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageIn req;
  req.run(argc, argv);
  return 0;
}
