/******************************************************************************
 *                      Services.cpp
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
 * @(#)$RCSfile: Services.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2004/10/11 13:43:48 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "Services.hpp"
#include "Factories.hpp"
#include "Constants.hpp"
#include "ISvcFactory.hpp"
#include "IService.hpp"
#include "IAddress.hpp"
#include "IObject.hpp"
#include "ICnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::Services::Services() {}

//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::Services::~Services() {
  std::map<const std::string, IService*>::iterator it;
  for (it = m_services.begin(); it != m_services.end(); it++) {
    it->second->release();
  }
  m_services.erase(m_services.begin(), m_services.end());
}

//-----------------------------------------------------------------------------
// service
//-----------------------------------------------------------------------------
castor::IService* castor::Services::service(const std::string name,
                                            const unsigned int id)
  throw(castor::exception::Exception) {
  std::map<const std::string, IService*>::const_iterator it =
    m_services.find(name);
  if (it == m_services.end()) {
    if (id > 0) {
      // build the service using the associated factory
      const ISvcFactory* fac =
        castor::Factories::instance()->factory(id);
      if (fac == 0) return 0;
      IService* svc = fac->instantiate(name);
      if (0 == svc) {
        return 0;
      } else {
        m_services[name] = svc;
      }
    } else {
      // No id given to create the service
      return 0;
    }
  } else {
    // add a reference to the existing service
    m_services[name]->addRef();
  }
  return m_services[name];
}

//-----------------------------------------------------------------------------
// cnvService
//-----------------------------------------------------------------------------
castor::ICnvSvc* castor::Services::cnvService(const std::string name,
                                              const unsigned int id)
  throw(castor::exception::Exception) {
  IService* svc = service(name, id);
  if (0 == svc) {
    return 0;
  }
  ICnvSvc* cnvSvc = dynamic_cast<ICnvSvc*>(svc);
  if (0 == cnvSvc) {
    svc->release();
    return 0;
  }
  return cnvSvc;
}

// -----------------------------------------------------------------------
// removeService
// -----------------------------------------------------------------------
void castor::Services::removeService(const std::string name) throw() {
  m_services.erase(name);
}

// -----------------------------------------------------------------------
// createRep
// -----------------------------------------------------------------------
void castor::Services::createRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool autocommit)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->createRep(address, object, autocommit);
}

// -----------------------------------------------------------------------
// updateRep
// -----------------------------------------------------------------------
void castor::Services::updateRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool autocommit)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->updateRep(address, object, autocommit);
}

// -----------------------------------------------------------------------
// deleteRep
// -----------------------------------------------------------------------
void castor::Services::deleteRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool autocommit)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->deleteRep(address, object, autocommit);
}

// -----------------------------------------------------------------------
// cnvSvcFromAddress
// -----------------------------------------------------------------------
castor::ICnvSvc*
castor::Services::cnvSvcFromAddress(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // check address
  if (0 == address) {
    castor::exception::Internal ex;
    ex.getMessage() << "No appropriate converter for a null address !";
    throw ex;
  }
  // Look for an adapted conversion service
  castor::ICnvSvc *cnvSvc =
    cnvService(address->cnvSvcName(), address->cnvSvcType());
  if (0 == cnvSvc) {
    castor::exception::Internal ex;
    ex.getMessage() << "No conversion service with name "
                    << address->cnvSvcName() << " and type "
                    << address->cnvSvcType();
    throw ex;
  }
  return cnvSvc;
}

// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::Services::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  return cnvSvc->createObj(address);
}


//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::Services::updateObj(castor::IAddress* address,
                                 castor::IObject* object)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->updateObj(object);
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void castor::Services::commit(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->commit();
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::Services::rollback(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->rollback();
}
