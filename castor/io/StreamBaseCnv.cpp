/******************************************************************************
 *                      StreamBaseCnv.cpp
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
 * @(#)$RCSfile: StreamBaseCnv.cpp,v $ $Revision: 1.13 $ $Release$ $Date: 2004/11/05 17:47:24 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/IObject.hpp"
#include "castor/exception/Internal.hpp"

// Local Files
#include "StreamBaseCnv.hpp"
#include "StreamCnvSvc.hpp"
#include "StreamAddress.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::io::StreamBaseCnv::StreamBaseCnv(castor::ICnvSvc* cnvSvc) :
  BaseObject(),
  m_cnvSvc(0) {
  m_cnvSvc = dynamic_cast<castor::io::StreamCnvSvc*>(cnvSvc);
  if (0 == m_cnvSvc) {
    castor::exception::Internal ex;
    ex.getMessage() << "No StreamCnvSvc available";
    throw ex;
  }
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::io::StreamBaseCnv::~StreamBaseCnv() throw() {}

// -----------------------------------------------------------------------
// RepType
// -----------------------------------------------------------------------
const unsigned int castor::io::StreamBaseCnv::RepType() {
  return castor::REP_STREAM;
}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
inline const unsigned int castor::io::StreamBaseCnv::repType() const {
  return RepType();
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamBaseCnv::updateRep(castor::IAddress* address,
                                          castor::IObject* object,
                                          bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update representation in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::io::StreamBaseCnv::deleteRep(castor::IAddress* address,
                                          castor::IObject* object,
                                          bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot delete representation in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamBaseCnv::updateObj(castor::IObject* obj)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// fillRep
//------------------------------------------------------------------------------
void castor::io::StreamBaseCnv::fillRep(castor::IAddress* address,
                                        castor::IObject* object,
                                        unsigned int type,
                                        bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "fillRep should never be called in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// fillObj
//------------------------------------------------------------------------------
void castor::io::StreamBaseCnv::fillObj(castor::IAddress* address,
                                        castor::IObject* object,
                                        unsigned int type)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "fillObj should never be called in case of streaming."
                  << std::endl;
  throw ex;
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::io::StreamCnvSvc* castor::io::StreamBaseCnv::cnvSvc() const {
  return m_cnvSvc;
}

