/******************************************************************************
 *                      Factories.hpp
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
 * @(#)$RCSfile: Factories.hpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_FACTORIES_HPP 
#define CASTOR_FACTORIES_HPP 1

//Include Files
#include <map>

namespace castor {

  // Forward declarations
  class ISvcFactory;

  /**
   * A class holding the list of available factories
   * to create services on demand.
   */
  class Factories {

  public:

    /**
     * gets the unique Factories instance
     */
    static Factories* instance();

    /**
     * finalizes all factories
     */
    static void finalize() throw();

    /**
     * adds a factory to the table
     */
    void addFactory (const ISvcFactory* factory) throw();

    /**
     *  gets a factory by id
     */
    const ISvcFactory* factory(const unsigned int id) throw();
    
  protected:

    /** Default destructor */
    ~Factories() throw();

  private:
    /** the list of service factories, by id */
    std::map<const unsigned int, const ISvcFactory*> m_factories;

  };

} // end of namespace castor

#endif // CASTOR_FACTORIES_HPP
