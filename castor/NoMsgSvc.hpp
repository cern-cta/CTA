/******************************************************************************
 *                      NoMsgSvc.hpp
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
 * @(#)$RCSfile: NoMsgSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/12 14:19:02 $ $Author: sponcec3 $
 *
 * A message service writing nowhere
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_NOMSGSVC_HPP 
#define CASTOR_NOMSGSVC_HPP 1

// Local Includes
#include "castor/MsgSvc.hpp"

namespace castor {

  /**
   * A message service not logging at all
   */
  class NoMsgSvc : public MsgSvc {

  public:
    
    /**
     * Constructor
     */
    NoMsgSvc(const std::string name) throw();
    
    /**
     * Destructor
     */
    virtual ~NoMsgSvc() throw() {};

    /**
     * Get the service id
     */
    virtual const unsigned int id() const;

    /**
     * Get the service id, statically
     */
    static const unsigned int ID();

  };
  
} // end of namespace castor

#endif // CASTOR_NOMSGSVC_HPP
