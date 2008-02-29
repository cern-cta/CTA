/******************************************************************************
 *                      BaseSynchronizationThread.cpp
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
 * @(#)$RCSfile: BaseSynchronizationThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/29 11:01:56 $ $Author: mmartins $
 *
 * Base Class for the Synchronization threads (NameServerSynchronizationThread and StagerSynchronizationThread)
 * used to check periodically whether files need to be deleted 
 *
 * @author castor dev team
 *****************************************************************************/

// Include files
#include "castor/gc/BaseSynchronizationThread.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/System.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "getconfent.h"

#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <map>





//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::BaseSynchronizationThread() throw() {};

//-----------------------------------------------------------------------------
// Run: Defined as a pure virtual method
// To be really implemented in the NameServerSynchronizationThread
// and in the StagerSynchronizationThread 
//-----------------------------------------------------------------------------


//-----------------------------------------------------------------------------
// readConfigFile
//-----------------------------------------------------------------------------
void castor::gc::BaseSynchronizationThread::readConfigFile
( bool firstTime) throw() {
  // synchronization interval
  char* value;
  int intervalnew;
  if ((value = getenv(syncIntervalConf.c_str())) || 
      (value = getconfent("GC",syncIntervalConf.c_str() , 0))) {
    intervalnew = atoi(value);
    if (intervalnew >= 0) {
      if ((unsigned int)intervalnew != syncInterval) {
	syncInterval = intervalnew;
	if (!firstTime) {
	  // "New synchronization interval"
	  castor::dlf::Param params[] =
	    { castor::dlf::Param("Getting configuration for:", syncIntervalConf),
	      castor::dlf::Param("New synchronization interval", syncInterval)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 21, 2, params);
	}
      }
    } else {
      // "Invalid value for synchronization interval. Used default"
      castor::dlf::Param params[] =
        { castor::dlf::Param("Getting configuration for:", syncIntervalConf),
	  castor::dlf::Param("Wrong value", value),
	  castor::dlf::Param("Value used", syncInterval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 19, 3, params);
    }
  } 
  // chunk size
  int chunkSizenew;
  if ((value = getenv(chunkSizeConf.c_str())) || 
      (value = getconfent("GC", chunkSizeConf.c_str(), 0))) {
    chunkSizenew = atoi(value);
    if (chunkSizenew >= 0) {
      if (chunkSize != (unsigned int)chunkSizenew) {
	chunkSize = (unsigned int)chunkSizenew;
	if (!firstTime) {
	  // "New synchronization chunk size"
	  castor::dlf::Param params[] =
	    { castor::dlf::Param("Getting configuration for:", chunkSizeConf),
	      castor::dlf::Param("New chunk size", chunkSize)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 22, 2, params);
	}
      }
    } else {
      // "Invalid value for chunk size. Used default"
      castor::dlf::Param params[] =
        { castor::dlf::Param("Getting configuration for:",chunkSizeConf),
	  castor::dlf::Param("Wrong value", value),
         castor::dlf::Param("Value used", chunkSize)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 3, params);
    }
  } 
  // logging at start time
  if (firstTime) {
    // "Synchronization configuration"
    castor::dlf::Param params[] =
      { castor::dlf::Param("Getting configuration for:",this->type),
	castor::dlf::Param("Interval", syncInterval),
       castor::dlf::Param("Chunk size", chunkSize)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 30, 2, params);
  }
}



//-----------------------------------------------------------------------------
// synchronizeFiles: Defined as a pure virtual method
// To be really implemented in the NameServerSynchronizationThread
// and in the StagerSynchronizationThread 
//-----------------------------------------------------------------------------

 
