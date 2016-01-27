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

#include "Converters.hpp"
#include "ICnvFactory.hpp"
#include "IConverter.hpp"

/** the unique and single factory table */
castor::Converters* g_ConvertersInstance = NULL;

/** The janitor in charge of clening up the converters instance:
 * it might or might not give the initial kick that triggers the 
 * lazy initialization of the converter, but he will for sure 
 * delete it.
 */
namespace castor {
  class ConvertersCleaner {
  public:
    ConvertersCleaner() {
      /* This might or might not trigger the lazy initialization */
      m_converters = castor::Converters::instance();
    }
    ~ConvertersCleaner() {
      delete m_converters;
    }
  private:
    Converters * m_converters;
  };
  static ConvertersCleaner g_converterCleaner;
}

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
      // delete it2->second;
    }
    convs.erase(convs.begin(), convs.end());
  }
  m_factories.erase (m_factories.begin(), m_factories.end());
}

//-----------------------------------------------------------------------------
// instance
//-----------------------------------------------------------------------------
castor::Converters* castor::Converters::instance()    {
  if (0 == g_ConvertersInstance) {
    g_ConvertersInstance = new castor::Converters();
  }
  return g_ConvertersInstance;
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
// cnvFactory
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

