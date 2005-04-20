/******************************************************************************
 *                      Busy.hpp
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
 * @(#)$RCSfile: Busy.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/20 14:03:45 $ $Author: sponcec3 $
 *
 * Busy exception
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef EXCEPTION_BUSY_HPP 
#define EXCEPTION_BUSY_HPP 1

// Include Files
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace exception {

    /**
     * Invalid argument exception
     */
    class Busy : public castor::exception::Exception {
      
    public:
      
      /**
       * default constructor
       */
      Busy();

    };
      
  } // end of namespace exception

} // end of namespace castor

#endif // EXCEPTION_BUSY_HPP
