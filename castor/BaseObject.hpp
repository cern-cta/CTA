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
 * @(#)$RCSfile: BaseObject.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2004/07/08 08:26:34 $ $Author: sponcec3 $
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
     * constructor
     */
    BaseObject() throw();
    
    /**
     * destructor
     */
    virtual ~BaseObject() throw();
    
    /**
     * Static access to the underlying Services object
     */
    static Services* services() throw(castor::exception::Exception);
    
    /**
     * Non static access to the underlying Services object
     */
    Services* svcs() throw(castor::exception::Exception);
    
    /**
     * gets a given message service from its name.
     * Note that the service has to be released after usage
     * @param name the name of the MsgSvc to retrieve
     * @return a pointer to the message service. This is always
     * a valid pointer if no exception was raised
     * @exception Exception if something went wrong
     */
    MsgSvc* msgSvc(std::string name) throw(castor::exception::Exception);

    /**
     * Sets the name of the MsgSvc to use for logging
     * Note that this name should be set only once if DLF
     * is used due to limitations in the current implementation
     * of DLF itsef
     */
    void initLog(std::string name) throw();

    /**
     * Access to the log stream
     */
    castor::logstream& clog() throw(castor::exception::Exception);

  private:
    
    /**
     * gets the thread local storage
     */
    static void getTLS(void** thip) throw(castor::exception::Exception);

  private:

    /**
     * The name of the MsgSvc to use
     */
    static std::string s_msgSvcName;

  }; // end of class BaseObject

} // end of namespace castor  

#endif // CASTOR_BASEOBJECT_HPP
