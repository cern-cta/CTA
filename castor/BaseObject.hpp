/******************************************************************************
 *                      BaseObject.hpp
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
 * @(#)$RCSfile: BaseObject.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/05/19 16:37:14 $ $Author: sponcec3 $
 *
 * Basic object support, including pointer to Services and log support
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEOBJECT_HPP 
#define CASTOR_BASEOBJECT_HPP 1

// Include Files
#include "castor/logstream.h"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class Services;
  class MsgSvc;

  /**
   * Basic object support, including pointer to Services and log support
   */
  class BaseObject {
    
  public:
    
    /**
     * Access to the underlying Services object
     */
    Services* svcs() throw(castor::exception::Exception);
    
    /**
     * gets the message service.
     * Note that the service has to be released after usage
     * @return a pointer to the message service or 0 if none
     * is available.
     */
    MsgSvc* msgSvc() throw(castor::exception::Exception);
    
    /**
     * Access to the log stream
     */
    castor::logstream& clog() throw(castor::exception::Exception);

  private:
    
    /**
     * gets the thread local storage
     */
    void getTLS(void** thip) throw(castor::exception::Exception);

  }; // end of class BaseObject

} // end of namespace castor  

#endif // CASTOR_BASEOBJECT_HPP
