/******************************************************************************
 *                      castor/stager/DiskCopyForRecallCInt.cpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_create
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_create(castor::stager::DiskCopyForRecall** obj) {
    *obj = new castor::stager::DiskCopyForRecall();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_delete
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_delete(castor::stager::DiskCopyForRecall* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_getDiskCopy
  //----------------------------------------------------------------------------
  castor::stager::DiskCopy* Cstager_DiskCopyForRecall_getDiskCopy(castor::stager::DiskCopyForRecall* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_fromDiskCopy
  //----------------------------------------------------------------------------
  castor::stager::DiskCopyForRecall* Cstager_DiskCopyForRecall_fromDiskCopy(castor::stager::DiskCopy* obj) {
    return dynamic_cast<castor::stager::DiskCopyForRecall*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_mountPoint
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_mountPoint(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->mountPoint().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setMountPoint
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setMountPoint(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setMountPoint(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_diskServer
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_diskServer(castor::stager::DiskCopyForRecall* instance, const char** var) {
    *var = instance->diskServer().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_DiskCopyForRecall_setDiskServer
  //----------------------------------------------------------------------------
  int Cstager_DiskCopyForRecall_setDiskServer(castor::stager::DiskCopyForRecall* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setDiskServer(snew_var);
    return 0;
  }

} // End of extern "C"
