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

#include "Factories.hpp"
#include "ISvcFactory.hpp"

/** the unique and single factory table */
castor::Factories* g_FactoriesInstance = NULL;

/** The janitor in charge of clening up the factories instance:
 * it might or might not give the initial kick that triggers the 
 * lazy initialization of the factory, but he will for sure 
 * delete it.
 */
namespace castor {
  class FactoriesCleaner {
  public:
    FactoriesCleaner() {
      /* This might or might not trigger the lazy initialization */
      m_factories = castor::Factories::instance();
    }
    ~FactoriesCleaner() {
      delete m_factories;
    }
  private:
    Factories * m_factories;
  };
  static FactoriesCleaner g_factoriesCleaner;
}

//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::Factories::~Factories() throw() {
  m_factories.erase (m_factories.begin(), m_factories.end());
}

//-----------------------------------------------------------------------------
// instance
//-----------------------------------------------------------------------------
castor::Factories* castor::Factories::instance()    {
  if (0 == g_FactoriesInstance) {
    g_FactoriesInstance = new Factories();
  }
  return g_FactoriesInstance;
}

//-----------------------------------------------------------------------------
// finalize
//-----------------------------------------------------------------------------
void castor::Factories::finalize() throw() {
  if (0 != g_FactoriesInstance) {
    delete g_FactoriesInstance;
    g_FactoriesInstance = 0;
  }
}

//-----------------------------------------------------------------------------
// addFactory
//-----------------------------------------------------------------------------
void castor::Factories::addFactory (const castor::ISvcFactory* factory)
  throw() {
  if (factory != 0) {
    const unsigned int id = factory->id();
    const ISvcFactory* fac = m_factories[id];
    if (fac != 0) delete fac;
    m_factories[id] = factory;
  }
}

//-----------------------------------------------------------------------------
// factory
//-----------------------------------------------------------------------------
const castor::ISvcFactory*
castor::Factories::factory(const unsigned int id) throw() {
  return m_factories[id];
}
