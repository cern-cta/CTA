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
 * @(#)$RCSfile: BaseCnvSvc.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/05/19 16:37:14 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
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

//-----------------------------------------------------------------------------
// addConverter
//-----------------------------------------------------------------------------
bool castor::BaseCnvSvc::addConverter(IConverter* pConverter) {
  if (pConverter !=0) {
    unsigned int id = pConverter->repType();
    if (m_converters[id] != 0) removeConverter(id);
    m_converters[id] = pConverter;
    return true;
  }
  return false;
}

//-----------------------------------------------------------------------------
// removeConverter
//-----------------------------------------------------------------------------
bool castor::BaseCnvSvc::removeConverter(const unsigned int id) {
  IConverter* c = m_converters[id];
  if (c != 0) delete c;
  m_converters.erase(id);
  return true;
}

//-----------------------------------------------------------------------------
// converter
//-----------------------------------------------------------------------------
castor::IConverter* castor::BaseCnvSvc::converter(const unsigned int objType)
  throw (castor::exception::Exception) {
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

  m_converters[objType] = fac->instantiate();
  if (0!= m_converters[objType]) return m_converters[objType];
  // Throw an exception since we did not find any suitable converter
  castor::exception::Internal e;
  e.getMessage() << "No converter for object type "
                 << objType << " and representation type "
                 << repType();
  throw e;
};

// -----------------------------------------------------------------------
// createRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::createRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   ObjectSet& alreadyDone,
                                   bool autocommit)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  IConverter* conv = converter(object->type());
  // convert
  return conv->createRep(address, object,
                         alreadyDone, autocommit);
}

// -----------------------------------------------------------------------
// updateRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::updateRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   ObjectSet& alreadyDone,
                                   bool autocommit)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  IConverter* conv = converter(object->type());
  // convert
  return conv->updateRep(address, object,
                         alreadyDone, autocommit);
}

// -----------------------------------------------------------------------
// deleteRep
// -----------------------------------------------------------------------
void castor::BaseCnvSvc::deleteRep(castor::IAddress* address,
                                   castor::IObject* object,
                                   ObjectSet& alreadyDone,
                                   bool autocommit)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  IConverter* conv = converter(object->type());
  // convert
  return conv->deleteRep(address, object,
                         alreadyDone, autocommit);
}

// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::BaseCnvSvc::createObj
(castor::IAddress* address, castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  castor::IConverter* conv = converter(address->objType());
  return conv->createObj(address, newlyCreated);
}
