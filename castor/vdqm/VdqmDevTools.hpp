/******************************************************************************
 *                      VdqmDevTools.hpp
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
 * @author Castor dev team
 *****************************************************************************/

#ifndef CASTOR_VDQM_VDQMDEBUGTOOLS_HPP
#define CASTOR_VDQM_VDQMDEBUGTOOLS_HPP 1

#include <iostream>


namespace castor {

  namespace vdqm {
  	
    /**
     * Collection of static methods to faciliate the development of the VDQM.
     */
    class VdqmDevTools {
    public:

      /**
       * Prints the specified VDQM request type using the specified output
       * stream.
       */
      static void printVdqmRequestType(std::ostream &os, const int type)
        throw();
    
    private:

      /**
       * Private constructor to prevent the instantiation of VdqmDevTool
       * objects.
       */
      VdqmDevTools() throw();

    }; // class VdqmDevTools

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_VDQMDEBUGTOOLS_HPP
