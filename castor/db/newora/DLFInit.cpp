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
 * @(#)$RCSfile: DLFInit.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/08/17 09:09:59 $ $Author: sponcec3 $
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
castor::db::ora::DLFInit DLFInitInstance;

//------------------------------------------------------------------------------
// DLFInit
//------------------------------------------------------------------------------
castor::db::ora::DLFInit::DLFInit() {
  castor::dlf::Message messages[] =
    {{ 0, "File was deleted while it was written to. Giving up with migration."},
     { -1, ""} };
  try {
    castor::dlf::dlf_addMessages(DLF_BASE_ORACLELIB, messages);
  } catch (castor::exception::Exception ex) {
    // We failed to insert our messages into DLF
    // So we cannot really log to DLF.
    // On the other hand, we cannot be sure that that standard out is usable.
    // So we have to ignore the error. Note really nice
  }
}
