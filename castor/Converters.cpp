/******************************************************************************
 *                      Converters.cpp
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
 * @(#)$RCSfile: Converters.cpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "Converters.hpp"
#include "ICnvFactory.hpp"
#include "IConverter.hpp"

/** the unique and single factory table */
castor::Converters* ConvertersInstance = 0;

//-----------------------------------------------------------------------------
// Default destructor
//-----------------------------------------------------------------------------
castor::Converters::~Converters() {
  std::map<const unsigned int,
    std::map<const unsigned int,
    const ICnvFactory*> >::iterator it;
  for (it = m_factories.begin(); it != m_factories.end(); it++) {
    std::map<const unsigned int, const ICnvFactory*>& convs = it->second;
    std::map<const unsigned int, const ICnvFactory*>::iterator it2;
    for (it2 = convs.begin(); it2 != convs.end(); it2++) {
      delete it2->second;
    }
    convs.erase(convs.begin(), convs.end());
  }
  m_factories.erase (m_factories.begin(), m_factories.end());
}

//-----------------------------------------------------------------------------
// instance
//-----------------------------------------------------------------------------
castor::Converters* castor::Converters::instance()    {
  if (0 == ConvertersInstance) {
    ConvertersInstance = new castor::Converters();
  }
  return ConvertersInstance;
}

//-----------------------------------------------------------------------------
// addFactory
//-----------------------------------------------------------------------------
void castor::Converters::addFactory (const ICnvFactory* factory) {
  if (factory != 0) {
    const unsigned int repType = factory->repType();
    const unsigned int objType = factory->objType();
    if (m_factories.find(repType) != m_factories.end()) {
      if (m_factories[repType].find(objType) != m_factories[repType].end()) {
        delete m_factories[repType][objType];
      }
    }
    m_factories[repType][objType] = factory;
  }
}

//-----------------------------------------------------------------------------
// converter
//-----------------------------------------------------------------------------
const castor::ICnvFactory*
castor::Converters::cnvFactory(const unsigned int repType,
                               const unsigned int objType) {
  // build the converter using the associated factory
  if (m_factories.find(repType) != m_factories.end()) {
    if (m_factories[repType].find(objType) != m_factories[repType].end()) {
      return m_factories[repType][objType];
    }
  }
  return 0;
}

