/******************************************************************************
 *                      BaseAddress.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEADDRESS_HPP 
#define CASTOR_BASEADDRESS_HPP 1

// Include Files
#include "IAddress.hpp"
#include "Constants.hpp"

namespace castor {

  /**
   * The most basic address : only a type.
   */
  class BaseAddress : public IAddress {

  public:

    /**
     * constructor
     */
    BaseAddress(const std::string cnvSvcName,
                const unsigned int cnvSvcType,
                const unsigned int objType = OBJ_INVALID);

    /*
     * destructor
     */
    virtual ~BaseAddress() throw() {}

    /**
     * gets the object type, that is the type of
     * object whose representation is pointed to by
     * this address.
     */
    virtual const unsigned int objType() const { return m_objType; }
    
    /**
     * sets the object type, that is the type of
     * object whose representation is pointed to by
     * this address.
     */
    void setObjType(unsigned int type) { m_objType = type; }
    
    /**
     * gets the name of the conversion service able
     * to deal with this address
     */
    virtual const std::string cnvSvcName() const { return m_cnvSvcName; }

    /**
     * gets the type of the conversion service able
     * to deal with this address
     */
    virtual const unsigned int cnvSvcType() const { return m_cnvSvcType; }

    /**
     * prints the address into an output stream
     */
    virtual void print(std::ostream& s) const;

  private:

    /**
     * the object type of this address
     */
    unsigned long m_objType;
    
    /**
     * the name of the conversion service able to deal
     * with this address
     */
    const std::string m_cnvSvcName;

    /**
     * the type of the conversion service able to deal
     * with this address
     */
    const unsigned int m_cnvSvcType;

  };

} // end of namespace castor

#endif // CASTOR_BASEADDRESS_HPP
