/******************************************************************************
 *                      IClientFactory.cpp
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
 * @(#)$RCSfile: IClientFactory.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/12/16 11:01:15 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IClientFactory.hpp"
#include "castor/rh/Client.hpp"
#include "castor/IClient.hpp"
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include <sstream>


//------------------------------------------------------------------------------
// client2String
//------------------------------------------------------------------------------
const std::string castor::IClientFactory::client2String
(const castor::IClient &cl)
  throw (castor::exception::Exception) {
  switch (cl.type()) {
  case castor::OBJ_Client :
    {
      const castor::rh::Client *c =
        dynamic_cast<const castor::rh::Client*>(&cl);
      std::ostringstream res;
      res << cl.type() << ":"
          << ((c->ipAddress() & 0xFF000000) >> 24) << "."
          << ((c->ipAddress() & 0x00FF0000) >> 16) << "."
          << ((c->ipAddress() & 0x0000FF00) >> 8) << "."
          << ((c->ipAddress() & 0x000000FF))
          << ":" << c->port();
      return res.str();
    }
  default:
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unknown Client type "
                   << castor::ObjectsIdStrings[cl.type()]
                   << std::endl
                   << "Cannot convert to string";
    throw e;
  }
}

//------------------------------------------------------------------------------
// string2Client
//------------------------------------------------------------------------------
castor::IClient* string2Client(const std::string &st)
  throw (castor::exception::Exception) {
  std::istringstream in(st);
  unsigned int type;
  in >> type;
  if (in.good()) { 
    switch (type) {
    case castor::OBJ_Client :
      {
        char c = 0;
        in >> c;
        if (c == ':') {
          unsigned short a[4];
          for (int i = 0; i < 4; i++) {
            in >> a[i];
            if (a[i] > 255) {
              castor::exception::InvalidArgument e;
              e.getMessage() << "Invalid IP address in Client string : \""
                             << st << "\"" << std::endl
                             << "Cannot convert string to Client";
              throw e;
            }
            if (i < 3) {
              c = 0;
              in >> c;
              if (c != '.') {
                castor::exception::InvalidArgument e;
                e.getMessage() << "Invalid IP address in Client string : \""
                               << st << "\"" << std::endl
                               << "Cannot convert string to Client";
                throw e;
              }
            }
          }
          c = 0;
          in >> c;
          if (c == ':') {
            unsigned short port = 0;
            in >> port;
            if (in) {
              castor::rh::Client *res = new castor::rh::Client();
              res->setIpAddress
                ((a[0] << 24) + (a[1] << 16) + (a[2] << 8) + a[3]);
              res->setPort(port);
              return res;
            }
          }
        }
      }
      break;
    default:
      castor::exception::InvalidArgument e;
      e.getMessage() << "Unknown type " << type
                     << " for a client" << std::endl
                     << "Cannot convert string to IClient";
      throw e;
    }
    
  }
  castor::exception::InvalidArgument e;
  e.getMessage() << "Unable to parse Client string : \""
                 << st << "\"" << std::endl
                 << "Cannot create Object";
  throw e;
}
