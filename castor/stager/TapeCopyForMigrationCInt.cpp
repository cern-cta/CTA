/******************************************************************************
 *                      castor/stager/TapeCopyForMigrationCInt.cpp
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
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapeCopyForMigration.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_create
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_create(castor::stager::TapeCopyForMigration** obj) {
    *obj = new castor::stager::TapeCopyForMigration();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_delete
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_delete(castor::stager::TapeCopyForMigration* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_getTapeCopy
  //----------------------------------------------------------------------------
  castor::stager::TapeCopy* Cstager_TapeCopyForMigration_getTapeCopy(castor::stager::TapeCopyForMigration* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_fromTapeCopy
  //----------------------------------------------------------------------------
  castor::stager::TapeCopyForMigration* Cstager_TapeCopyForMigration_fromTapeCopy(castor::stager::TapeCopy* obj) {
    return dynamic_cast<castor::stager::TapeCopyForMigration*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_TapeCopyForMigration_getIObject(castor::stager::TapeCopyForMigration* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::TapeCopyForMigration* Cstager_TapeCopyForMigration_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::TapeCopyForMigration*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_print
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_print(castor::stager::TapeCopyForMigration* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_TYPE
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_TYPE(int* ret) {
    *ret = castor::stager::TapeCopyForMigration::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_type
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_type(castor::stager::TapeCopyForMigration* instance,
                                        int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_diskServer
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_diskServer(castor::stager::TapeCopyForMigration* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_setDiskServer(castor::stager::TapeCopyForMigration* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_mountPoint
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_mountPoint(castor::stager::TapeCopyForMigration* instance, const char** var) {
    *var = instance->mountPoint().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopyForMigration_setMountPoint
  //----------------------------------------------------------------------------
  int Cstager_TapeCopyForMigration_setMountPoint(castor::stager::TapeCopyForMigration* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMountPoint(snew_var);
    return 0;
  }

} // End of extern "C"
