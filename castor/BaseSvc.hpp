/******************************************************************************
 *                      BaseSvc.hpp
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
 * @(#)$RCSfile: BaseSvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/05 17:47:19 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASESVC_HPP 
#define CASTOR_BASESVC_HPP 1

// Include Files
#include <string>

// Local Includes
#include "IService.hpp"
#include "BaseObject.hpp"

namespace castor {

  /**
   * Base class for services.
   * It essentially handles a reference count
   */
  class BaseSvc : public BaseObject, public virtual IService {

  public:

    /**
     * Constructor
     */
    BaseSvc(const std::string name) throw();

    /**
     * Destructor
     */
    virtual ~BaseSvc() throw() {};
    
    /**
     * Get the service name
     */
    virtual const std::string name() const { return m_name; }

    /**
     * adds a reference
     */
    virtual void addRef() throw() { m_refs++; };
    
    /**
     * removes a reference to the service and releases it
     * if the count goes to 0
     */
    virtual void release() throw();

  private:

    /** the service name */
    const std::string m_name;
    
    /** the reference counter */
    unsigned int m_refs;
    
  };
  
} // end of castor namespace

#endif // CASTOR_BASESVC_HPP
