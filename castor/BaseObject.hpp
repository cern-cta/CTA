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
 * @(#)$RCSfile: BaseObject.hpp,v $ $Revision: 1.11 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * Basic object support, including pointer to Services and log support
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEOBJECT_HPP 
#define CASTOR_BASEOBJECT_HPP 1

// Include Files
#include "castor/logstream.h"
#include "castor/dlf/Dlf.hpp"
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
     * @param name the type of the MsgSvc to retrieve
     * @return a pointer to the message service. This is always
     * a valid pointer if no exception was raised. The caller
     * takes the responsability of releasing the service
     * after usage
     * @exception Exception if something went wrong
     */
    static MsgSvc* msgSvc(std::string name,
                          const unsigned long id)
      throw(castor::exception::Exception);

    /**
     * Defines which logging service should be used in
     * the future by giving a name and a service type.
     * Note that this method should only be called once.
     * In case of other calls, they will be ignored and
     * a warning will be issued in the already configured
     * log
     */
    static void initLog(std::string name,
                        const unsigned long id) throw();

    /**
     * Access to the log stream
     */
    castor::logstream& clog() throw(castor::exception::Exception);

    /**
     * gets the thread local storage for a given key
     */
    static void getTLS(int* key, void** thip)
      throw(castor::exception::Exception);

  protected:

    /**
     * The name of the MsgSvc to use
     */
    static std::string s_msgSvcName;

    /**
     * The type of MsgSvc to use
     */
    static unsigned long s_msgSvcId;

  }; // end of class BaseObject

} // end of namespace castor  

#endif // CASTOR_BASEOBJECT_HPP
