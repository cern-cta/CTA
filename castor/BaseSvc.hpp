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
 * @(#)$RCSfile: BaseSvc.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2005/10/07 16:05:53 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASESVC_HPP 
#define CASTOR_BASESVC_HPP 1

// Include Files
#include <string>
#include <set>

// Local Includes
#include "IService.hpp"
#include "BaseObject.hpp"

namespace castor {

  /**
   * Base class for services.
   * It essentially handles a reference count
   */
  class BaseSvc : public virtual BaseObject,
                  public virtual IService {

  public:

    /**
     * Constructor
     */
    BaseSvc(const std::string name) throw();

    /**
     * Destructor
     */
    virtual ~BaseSvc() throw();
    
    /**
     * Get the service name
     */
    virtual const std::string name() const { return m_name; }

    /**
     * Registration of a dependent service.
     */
    virtual void registerDepSvc(castor::IService* svc)
      throw() { m_depSvcs.insert(svc); }

    /**
     * Unregistration of a dependent service.
     */
    virtual void unregisterDepSvc(castor::IService* svc)
      throw() { m_depSvcs.erase(svc); }

    /**
     * resets the service
     */
    virtual void reset() throw();

  private:

    /** the service name */
    const std::string m_name;

    /** a list of dependent services */
    std::set<castor::IService*> m_depSvcs;

  };
  
} // end of castor namespace

#endif // CASTOR_BASESVC_HPP
