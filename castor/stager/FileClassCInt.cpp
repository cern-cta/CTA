/******************************************************************************
 *                      castor/stager/FileClassCInt.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/FileClass.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_FileClass_create
  //----------------------------------------------------------------------------
  int Cstager_FileClass_create(castor::stager::FileClass** obj) {
    *obj = new castor::stager::FileClass();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_FileClass_delete
  //----------------------------------------------------------------------------
  int Cstager_FileClass_delete(castor::stager::FileClass* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_FileClass_getIObject(castor::stager::FileClass* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::FileClass* Cstager_FileClass_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::FileClass*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_print
  //----------------------------------------------------------------------------
  int Cstager_FileClass_print(castor::stager::FileClass* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_TYPE
  //----------------------------------------------------------------------------
  int Cstager_FileClass_TYPE(int* ret) {
    *ret = castor::stager::FileClass::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_setId
  //----------------------------------------------------------------------------
  int Cstager_FileClass_setId(castor::stager::FileClass* instance,
                              u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_id
  //----------------------------------------------------------------------------
  int Cstager_FileClass_id(castor::stager::FileClass* instance,
                           u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_type
  //----------------------------------------------------------------------------
  int Cstager_FileClass_type(castor::stager::FileClass* instance,
                             int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_name
  //----------------------------------------------------------------------------
  int Cstager_FileClass_name(castor::stager::FileClass* instance, const char** var) {
    *var = instance->name().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_setName
  //----------------------------------------------------------------------------
  int Cstager_FileClass_setName(castor::stager::FileClass* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setName(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_minFileSize
  //----------------------------------------------------------------------------
  int Cstager_FileClass_minFileSize(castor::stager::FileClass* instance, unsigned int* var) {
    *var = instance->minFileSize();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_setMinFileSize
  //----------------------------------------------------------------------------
  int Cstager_FileClass_setMinFileSize(castor::stager::FileClass* instance, unsigned int new_var) {
    instance->setMinFileSize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_maxFileSize
  //----------------------------------------------------------------------------
  int Cstager_FileClass_maxFileSize(castor::stager::FileClass* instance, unsigned int* var) {
    *var = instance->maxFileSize();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_setMaxFileSize
  //----------------------------------------------------------------------------
  int Cstager_FileClass_setMaxFileSize(castor::stager::FileClass* instance, unsigned int new_var) {
    instance->setMaxFileSize(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_nbCopies
  //----------------------------------------------------------------------------
  int Cstager_FileClass_nbCopies(castor::stager::FileClass* instance, unsigned int* var) {
    *var = instance->nbCopies();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_FileClass_setNbCopies
  //----------------------------------------------------------------------------
  int Cstager_FileClass_setNbCopies(castor::stager::FileClass* instance, unsigned int new_var) {
    instance->setNbCopies(new_var);
    return 0;
  }

} // End of extern "C"
