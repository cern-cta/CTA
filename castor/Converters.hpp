/******************************************************************************
 *                      Converters.hpp
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
 * @(#)$RCSfile: Converters.hpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_CONVERTERS_HPP
#define CASTOR_CONVERTERS_HPP 1

//Include Files
#include <map>

namespace castor {

  // Forward declarations
  class ICnvFactory;

  /**
   * A class holding the list of factories to create
   * converters on demand.
   */
  class Converters {

  public:
    /**
     * gets the unique Services instance
     */
    static Converters* instance();

    /**
     * adds a factory to the table
     */
    void addFactory (const ICnvFactory* factory);

    /**
     * gets a converter factory for a given type of representation
     * and a given type of object
     * @param repType the type of representation
     * @param objType the type of object
     */
    const ICnvFactory* cnvFactory (const unsigned int repType,
                                   const unsigned int objType);

  protected:
    /** Default constructor */
    Converters() {};

    /** Default destructor */
    virtual ~Converters();

  private:
    /** the list of factories */
    std::map<const unsigned int,
             std::map<const unsigned int,
                      const ICnvFactory*> > m_factories;
  };

} // end of namespace castor

#endif // CASTOR_CONVERTERS_HPP
