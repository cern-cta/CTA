/******************************************************************************
 *                      IClientFactoryCInt.cpp
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
 * @(#)$RCSfile: IClientFactoryCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/01/04 13:22:35 $ $Author: bcouturi $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IClientFactory.hpp"
#include "castor/exception/Exception.hpp"
#include <string>

extern "C" {
  
  //----------------------------------------------------------------------------
  // C_IClientFactory_client2String
  //----------------------------------------------------------------------------
  const char*  C_IClientFactory_client2String
  (const castor::IClient *cl, const char** errorMsg) {
    try {
      std::string res =
        castor::IClientFactory::client2String(*cl);
      return strdup(res.c_str());
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
    }
    return 0;
  }
    
  //----------------------------------------------------------------------------
  // C_IClientFactory_string2Client
  //----------------------------------------------------------------------------
  castor::IClient* C_IClientFactory_string2Client
  (char* st, const char** errorMsg) {
    try {
      return castor::IClientFactory::string2Client(st);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
    }
    return 0;    
  }
  
} // end of extern "C"
