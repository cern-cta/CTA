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
 * @(#)$RCSfile: StreamBaseCnv.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/11 13:43:52 $ $Author: sponcec3 $
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
castor::io::StreamBaseCnv::StreamBaseCnv() :
  BaseObject(),
  m_cnvSvc(0) {
  m_cnvSvc = dynamic_cast<castor::io::StreamCnvSvc*>
    (svcs()->cnvService("StreamCnvSvc", SVC_STREAMCNV));
  if (!m_cnvSvc) {
    castor::exception::Internal ex;
    ex.getMessage() << "No StreamCnvSvc available";
    throw ex;
  }
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::io::StreamBaseCnv::~StreamBaseCnv() throw() {
  m_cnvSvc->release();
}

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

// -----------------------------------------------------------------------
// unlinkChild
// -----------------------------------------------------------------------
void castor::io::StreamBaseCnv::unlinkChild
(const castor::IObject* parent,
 const castor::IObject* child)
  throw (castor::exception::Exception) {
  castor::exception::Internal e;
  e.getMessage() << "unlinkChild should never be called while streaming.";
  throw e;
}

// -----------------------------------------------------------------------
// cnvSvc
// -----------------------------------------------------------------------
castor::io::StreamCnvSvc* castor::io::StreamBaseCnv::cnvSvc() const {
  return m_cnvSvc;
}

