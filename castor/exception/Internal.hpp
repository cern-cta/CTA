/******************************************************************************
 *                      Internal.hpp
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
 * @(#)$RCSfile: Internal.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/19 16:37:22 $ $Author: sponcec3 $
 *
 * Internal error exception
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef EXCEPTION_INTERNAL_HPP 
#define EXCEPTION_INTERNAL_HPP 1

// Include Files
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace exception {

    /**
     * Invalid argument exception
     */
    class Internal : public castor::exception::Exception {
      
    public:
      
      /**
       * default constructor
       */
      Internal();

    };
      
  } // end of namespace exception

} // end of namespace castor

#endif // EXCEPTION_INTERNAL_HPP
