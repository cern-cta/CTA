/******************************************************************************
 *                      CnvFactory.hpp
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
 * @(#)$RCSfile: CnvFactory.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/05 17:47:20 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_CNVFACTORY_HPP
#define CASTOR_CNVFACTORY_HPP 1

// Include Files
#include "ICnvFactory.hpp"
#include "Converters.hpp"

namespace castor {

  // Forward Declarations
  class IConverter;
  class ICnvSvc;

  /**
   * A basic factory for converters
   */
  template<class Converter> class CnvFactory : public ICnvFactory {

  public:
    /** Default constructor */
    CnvFactory();

    /** Default destructor */
    virtual ~CnvFactory() {};

    /**
     * Instantiate the converter
     * @param name the object name. Actually ignored
     */
    virtual IConverter* instantiate(castor::ICnvSvc* cnvSvc,
                                    const std::string name = "") const;

    /**
     * gets the object type, that is the type of
     * object the underlying converter can convert
     */
    const unsigned int objType() const;

    /**
     * gets the representation type, that is the type of
     * the representation the underlying converter can deal with
     */
    const unsigned int repType() const;

  };

  template<class Converter> inline IConverter*
  CnvFactory<Converter>::instantiate(castor::ICnvSvc* cnvsvc,
                                     const std::string name) const {
    return new Converter(cnvsvc);
  }

  template<class Converter>
  inline const unsigned int CnvFactory<Converter>::objType() const {
    return Converter::ObjType();
  }

  template<class Converter>
  inline const unsigned int CnvFactory<Converter>::repType() const {
    return Converter::RepType();
  }

} // end of namespace castor

template<class Converter>
castor::CnvFactory<Converter>::CnvFactory() {
  castor::Converters::instance()->addFactory(this);
}

#endif // CASTOR_CNVFACTORY_HPP
