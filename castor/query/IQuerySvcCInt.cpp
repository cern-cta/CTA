/******************************************************************************
 *                      IQuerySvcCInt.cpp
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
 * @(#)$RCSfile: IQuerySvcCInt.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2005/11/11 10:32:26 $ $Author: itglp $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/query/IQuerySvc.hpp"
#include "castor/Services.hpp"
#include <errno.h>
#include <string>

extern "C" {
  
  // C include files
  #include "castor/ServicesCInt.hpp"
  #include "castor/query/IQuerySvcCInt.hpp"
  
  //----------------------------------------------------------------------------
  // checkIQuerySvc
  //----------------------------------------------------------------------------
  bool checkIQuerySvc(Cquery_IQuerySvc_t* qrySvc) {
    if (0 == qrySvc || 0 == qrySvc->qrySvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  // Cquery_IQuerySvc_fromIService
  //----------------------------------------------------------------------------
  Cquery_IQuerySvc_t*
  Cquery_IQuerySvc_fromIService(castor::IService* obj) {
    Cquery_IQuerySvc_t* qrySvc = new Cquery_IQuerySvc_t();
    qrySvc->errorMsg = "";
    qrySvc->qrySvc = dynamic_cast<castor::query::IQuerySvc*>(obj);
    return qrySvc;
  }

  //----------------------------------------------------------------------------
  // Cquery_IQuerySvc_delete
  //----------------------------------------------------------------------------
  int Cquery_IQuerySvc_delete(Cquery_IQuerySvc_t* qrySvc) {
    try {
      if (0 == qrySvc) return 0;
      if (0 != qrySvc->qrySvc) qrySvc->qrySvc->release();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      qrySvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete qrySvc;
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cquery_IQuerySvc_diskCopies4File
  //-------------------------------------------------------------------------
  int Cquery_IQuerySvc_diskCopies4File
  (struct Cquery_IQuerySvc_t* qrySvc,
   char* fileId, char* nsHost, u_signed64 svcClassId,
   castor::stager::DiskCopyInfo*** diskCopies,
   unsigned int* diskCopiesNb) {
    if (!checkIQuerySvc(qrySvc)) return -1;
    try {
      std::list<castor::stager::DiskCopyInfo*>* result =
        qrySvc->qrySvc->diskCopies4File(fileId, nsHost, svcClassId);
      if(0 == result) {
          *diskCopiesNb = 0;
          *diskCopies = 0;
          return 0;
      }
      *diskCopiesNb = result->size();
      *diskCopies = (castor::stager::DiskCopyInfo**)
        malloc((*diskCopiesNb) * sizeof(castor::stager::DiskCopyInfo*));
      std::list<castor::stager::DiskCopyInfo*>::iterator it =
        result->begin();
      for (int i = 0; i < *diskCopiesNb; i++, it++) {
        (*diskCopies)[i] = *it;
      }
      delete result;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      qrySvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

}
