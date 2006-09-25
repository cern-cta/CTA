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
 * @(#)$RCSfile: DLFInit.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/09/25 09:21:22 $ $Author: sponcec3 $
 *
 * Initialization of the DLF messages for the Shared memory part
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/sharedMemory/DLFInit.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"

//------------------------------------------------------------------------------
// DLFInitInstance
//------------------------------------------------------------------------------
castor::sharedMemory::DLFInit DLFInitInstance;

//------------------------------------------------------------------------------
// DLFInit
//------------------------------------------------------------------------------
castor::sharedMemory::DLFInit::DLFInit () {
  castor::dlf::Message messages[] =
    {{ 0, "Unable to get shared memory id. Giving up"},
     { 1, "Unable to create shared memory. Giving up"},
     { 2, "Created the shared memory."},
     { 3, "Unable to get pointer to shared memory. Giving up"},
     { 4, "Not enough space for header in shared memory. Giving up"}};
  castor::dlf::dlf_addMessages(DLF_BASE_SHAREDMEMORY, messages);
}
