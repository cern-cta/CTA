/******************************************************************************
 *                      DLFInit.cpp
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
 * @(#)$RCSfile: DLFInit.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2008/02/22 14:56:54 $ $Author: mmartins $
 *
 * Initialization of the DLF messages for the ORACLE part
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/db/newora/DLFInit.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/exception/Exception.hpp"

//------------------------------------------------------------------------------
// DLFInitInstance
//------------------------------------------------------------------------------
namespace castor {

  namespace db {

    namespace ora {

      castor::db::ora::DLFInit DLFInitInstance;

    }  // namespace ora

  }  // namespace db

} // namespace castor

//------------------------------------------------------------------------------
// DLFInit
//------------------------------------------------------------------------------
castor::db::ora::DLFInit::DLFInit() {
  castor::dlf::Message messages[] =
    {{ 0,  "File was deleted while it was written to. Giving up with migration."},
     { 1,  "Cleaning of out of date requests done"},
     { 2,  "Cleaning of failed diskCopies done"},
     { 3,  "Cleaning of archived requests done"},
     { 4,  "Cleaning of out of date requests failed"},
     { 5,  "Cleaning of failed diskCopies failed"},
     { 6,  "Cleaning of archived requests failed"},
     { 7,  "Cleaning Service not available"},
     { 8,  "Cleaning of out of date STAGEOUT diskCopies done"},
     { 9,  "Cleaning of out of date recalls done"},
     { 10, "Cleaning of out of date STAGEOUT diskCopies failed"},
     { 11, "Cleaning of out of date recalls failed"},
     { 12, "File was dropped by Cleaning Daemon"},
     { 13, "PutDone enforced by Cleaning Daemon"},
     { 14, "Recall canceled by Cleaning Daemon"},
     { 15, "Error caught in nsFilesDeleted"},
     { 16, "Error caught in stgFilesDeleted"},
     { -1, ""} };
  try {
    castor::dlf::dlf_addMessages(DLF_BASE_ORACLELIB, messages);
  } catch (castor::exception::Exception ex) {
    // We failed to insert our messages into DLF
    // So we cannot really log to DLF.
    // On the other hand, we cannot be sure that that standard out is usable.
    // So we have to ignore the error. Not really nice
  }
}
