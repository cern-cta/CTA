/******************************************************************************
 *                      BaseCnvSvc.cpp
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
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <vector>
#include <map>
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

// Local Includes
#include "ICnvFactory.hpp"
#include "BaseCnvSvc.hpp"
#include "Converters.hpp"
#include "IConverter.hpp"
#include "IAddress.hpp"
#include "IObject.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::BaseCnvSvc::BaseCnvSvc(const std::string name) :
  BaseSvc(name) {}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::BaseCnvSvc::~BaseCnvSvc() throw() {
  BaseCnvSvc::reset();
}

// -----------------------------------------------------------------------
// reset
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::reset() throw() {
  // release all converters
  std::map<unsigned int, IConverter*>::iterator it;
  for (it = m_converters.begin(); it != m_converters.end(); it++) {
    if (it->second != 0) {
      delete it->second;
    }
  }
  m_converters.clear();
  // call parent's reset
  BaseSvc::reset();
}

//-----------------------------------------------------------------------------
// addAlias
//-----------------------------------------------------------------------------
void castor::BaseCnvSvc::addAlias(const unsigned int alias,
                                  const unsigned int real) {
  m_aliases[alias] = real;
}

//-----------------------------------------------------------------------------
// removeAlias
//-----------------------------------------------------------------------------
void castor::BaseCnvSvc::removeAlias(const unsigned int id) {
  m_aliases.erase(id);
}

//-----------------------------------------------------------------------------
// converter
//-----------------------------------------------------------------------------
castor::IConverter* castor::BaseCnvSvc::converter
(const unsigned int origObjType)
  throw (castor::exception::Exception) {
  // First uses aliases
  unsigned int objType = origObjType;
  if (m_aliases.find(objType) != m_aliases.end()) {
    objType = m_aliases[objType];
  }
  // check if we have one
  IConverter* conv = m_converters[objType];
  if (0 != conv) return conv;
  // Try to find one
  const castor::ICnvFactory* fac =
    castor::Converters::instance()->cnvFactory(repType(), objType);
  if (0 == fac) {
    castor::exception::Internal e;
    e.getMessage() << "No factory found for object type "
                   << objType << " and representation type "
                   << repType();
    throw e;
  }
  m_converters[objType] = fac->instantiate(this);
  if (0!= m_converters[objType]) return m_converters[objType];
  // Throw an exception since we did not find any suitable converter
  castor::exception::Internal e;
  e.getMessage() << "No converter for object type "
                 << objType << " and representation type "
                 << repType();
  throw e;
}

// -----------------------------------------------------------------------
// createRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::createRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   bool endTransaction,
                                   unsigned int type)
  throw (castor::exception::Exception) {
  // If no object, nothing to create
  if (0 != object) {
    // Look for an adapted converter
    // The converter is always valid if no exception is thrown
    IConverter* conv = converter(object->type());
    // convert
    conv->createRep(address, object, endTransaction, type);
  }
}

// -----------------------------------------------------------------------
// bulkCreateRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::bulkCreateRep(castor::IAddress* address,
                                       std::vector<castor::IObject*> &objects,
                                       bool endTransaction,
                                       unsigned int type)
  throw (castor::exception::Exception) {
  // If no object, nothing to create
  if (objects.size() > 0) {
    // Look for an adapted converter
    // The converter is always valid if no exception is thrown
    // Note that we assume that all objects have the same type
    IConverter* conv = converter(objects[0]->type());
    // convert
    conv->bulkCreateRep(address, objects, endTransaction, type);
  }
}

// -----------------------------------------------------------------------
// updateRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::updateRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   bool endTransaction)
  throw (castor::exception::Exception) {
  // If no object, nothing to update
  if (0 != object) {
    // Look for an adapted converter
    // The converter is always valid if no exception is thrown
    IConverter* conv = converter(object->type());
    // convert
    conv->updateRep(address, object, endTransaction);
  }
}

// -----------------------------------------------------------------------
// deleteRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::deleteRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   bool endTransaction)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  IConverter* conv = converter(object->type());
  // convert
  conv->deleteRep(address, object, endTransaction);
}

// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::BaseCnvSvc::createObj
(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(address->objType());
  return conv->createObj(address);
}

// -----------------------------------------------------------------------
// bulkCreateObj
// -----------------------------------------------------------------------
std::vector<castor::IObject*> castor::BaseCnvSvc::bulkCreateObj
(castor::IAddress* address)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(address->objType());
  return conv->bulkCreateObj(address);
}

// -----------------------------------------------------------------------
// updateObj
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::updateObj(castor::IObject* object)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(object->type());
  return conv->updateObj(object);
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::BaseCnvSvc::fillRep(castor::IAddress* address,
                                 castor::IObject* object,
                                 unsigned int type,
                                 bool endTransaction)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(object->type());
  return conv->fillRep(address, object, type, endTransaction);
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::BaseCnvSvc::fillObj(castor::IAddress* address,
                                 castor::IObject* object,
                                 unsigned int type,
                                 bool endTransaction)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(object->type());
  return conv->fillObj(address, object, type, endTransaction);
}

// -----------------------------------------------------------------------
// deleteRepByAddress
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::deleteRepByAddress (castor::IAddress* address,
                                             bool endTransaction)
  throw (castor::exception::Exception) {
  castor::IObject* obj = createObj(address);
  address->setObjType(obj->type());
  deleteRep(address, obj, endTransaction);
}

//------------------------------------------------------------------------------
// commit
//------------------------------------------------------------------------------
void castor::BaseCnvSvc::commit()
  throw (castor::exception::Exception) {
  // Default implementation, does nothing.
}

//------------------------------------------------------------------------------
// rollback
//------------------------------------------------------------------------------
void castor::BaseCnvSvc::rollback()
  throw (castor::exception::Exception) {
  // Default implementation, does nothing.
}

//------------------------------------------------------------------------------
// createStatement
//------------------------------------------------------------------------------
castor::db::IDbStatement* castor::BaseCnvSvc::createStatement(const std::string&)
  throw (castor::exception::Exception) {
  // Default implementation, does nothing.
  return 0;
}
