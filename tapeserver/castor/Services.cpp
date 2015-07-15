/******************************************************************************
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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
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

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::Services::Services() {}

//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::Services::~Services() {
  // cleanup all remaining services
  for (std::map<const std::string, IService*>::const_iterator it =
         m_services.begin();
       it != m_services.end();
       it++) {
    delete it->second;
  }
  m_services.clear();
}

//-----------------------------------------------------------------------------
// service
//-----------------------------------------------------------------------------
castor::IService* castor::Services::service(const std::string name,
                                            const unsigned int id)
   {
  std::map<const std::string, IService*>::const_iterator it = m_services.find(name);
  if (it == m_services.end()) {
    if (id > 0) {
      // build the service using the associated factory
      const ISvcFactory* fac = castor::Factories::instance()->factory(id);
      // if no factory is available, complain
      if (0 == fac) {
        castor::exception::Exception e;
        e.getMessage() << "No factory found for object type " << id;
        throw e;
      }

      IService* svc = fac->instantiate(name);
      // if the service was not instantiated, complain
      if (0 == svc) {
        castor::exception::Exception e;
        e.getMessage() << "No service found for service " << name;
        throw e;
      } else {
        m_services[name] = svc;
      }
    } else {
      // No id given to create the service
      return 0;
    }
  }
  return m_services[name];
}

//-----------------------------------------------------------------------------
// cnvService
//-----------------------------------------------------------------------------
castor::ICnvSvc* castor::Services::cnvService(const std::string name,
                                              const unsigned int id)
   {
  IService* svc = service(name, id);
  if (0 == svc) {
    return 0;
  }
  ICnvSvc* cnvSvc = dynamic_cast<ICnvSvc*>(svc);
  if (0 == cnvSvc) {
    return 0;
  }
  return cnvSvc;
}

// -----------------------------------------------------------------------
// createRep
// -----------------------------------------------------------------------
void castor::Services::createRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->createRep(address, object, endTransaction);
}

// -----------------------------------------------------------------------
// bulkCreateRep
// -----------------------------------------------------------------------
void castor::Services::bulkCreateRep(castor::IAddress* address,
                                     std::vector<castor::IObject*> &objects,
                                     bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->bulkCreateRep(address, objects, endTransaction);
}

// -----------------------------------------------------------------------
// updateRep
// -----------------------------------------------------------------------
void castor::Services::updateRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->updateRep(address, object, endTransaction);
}

// -----------------------------------------------------------------------
// deleteRep
// -----------------------------------------------------------------------
void castor::Services::deleteRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->deleteRep(address, object, endTransaction);
}

// -----------------------------------------------------------------------
// cnvSvcFromAddress
// -----------------------------------------------------------------------
castor::ICnvSvc*
castor::Services::cnvSvcFromAddress(castor::IAddress* address)
   {
  // check address
  if (0 == address) {
    castor::exception::Exception ex;
    ex.getMessage() << "No appropriate converter for a null address !";
    throw ex;
  }
  // Look for an adapted conversion service
  castor::ICnvSvc *cnvSvc =
    cnvService(address->cnvSvcName(), address->cnvSvcType());
  if (0 == cnvSvc) {
    castor::exception::Exception ex;
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
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  castor::IObject* result = cnvSvc->createObj(address);
  return result;
}

// -----------------------------------------------------------------------
// bulkCreateObj
// -----------------------------------------------------------------------
std::vector<castor::IObject*>
castor::Services::bulkCreateObj(castor::IAddress* address)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  std::vector<castor::IObject*> result = cnvSvc->bulkCreateObj(address);
  return result;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::Services::updateObj(castor::IAddress* address,
                                 castor::IObject* object)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->updateObj(object);
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void castor::Services::commit(castor::IAddress* address)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->commit();
}


//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::Services::fillRep(castor::IAddress* address,
                               castor::IObject* object,
                               unsigned int type,
                               bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->fillRep(address, object, type, endTransaction);
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::Services::fillObj(castor::IAddress* address,
                               castor::IObject* object,
                               unsigned int type,
                               bool endTransaction)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->fillObj(address, object, type, endTransaction);
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::Services::rollback(castor::IAddress* address)
   {
  // Always returns a valid cnvSvc or throws an exception
  castor::ICnvSvc* cnvSvc = cnvSvcFromAddress(address);
  cnvSvc->rollback();
}
