/******************************************************************************
 *                      TapeStreamLinkerThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: TapeStreamLinkerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/tape/tapegateway/TapeStreamLinkerThread.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"


#include <u64subr.h>


  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeStreamLinkerThread::TapeStreamLinkerThread(castor::tape::tapegateway::ITapeGatewaySvc* svc){
  m_dbSvc=svc; 
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::TapeStreamLinkerThread::run(void* par)
{
 
  std::vector<castor::stager::Stream*> streamsToResolve;
  std::vector<castor::stager::Stream*>::iterator stream;

  // get streams to check from the db
  try {
     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 15, 0, NULL);
    streamsToResolve= m_dbSvc->getStreamsToResolve();
  } catch (castor::exception::Exception e) {
    // error in getting new tape to submit
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 2, params); 
    return;
  }

  // ask tapes to vmgr

  std::vector<u_signed64> strIds; 
  std::vector<std::string> vids;
  std::vector<castor::stager::Tape*> tapesUsed;
  std::vector<castor::stager::Tape*>::iterator tapeItem;
  castor::stager::Tape* tapeToUse=NULL;
  VmgrTapeGatewayHelper vmgrHelper;

  std::vector<castor::stager::Stream*>::iterator strItem=streamsToResolve.begin();

  while ( strItem != streamsToResolve.end() ) {

    // get tape from vmgr

    castor::dlf::Param params0[] =
      {castor::dlf::Param("StreamId",(*strItem)->id())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 17, 1, params0);
    try {
      tapeToUse=vmgrHelper.getTapeForStream(**strItem);
    } catch(castor::exception::Exception e) {
      strItem++;
      continue;
      // in case of errors we don't change the status from TO_BE_RESOLVED to TO_BE_SENT_TO_VDQM -- NO NEED OF WAITSPACE status
    }
    if ( tapeToUse != NULL ){ 
      // got the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("StreamId",(*strItem)->id()),
	 castor::dlf::Param("VID",tapeToUse->vid())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 18, 1, params);

      strIds.push_back((*strItem)->id());
      vids.push_back(tapeToUse->vid());
      tapesUsed.push_back(tapeToUse);
      
    }
   
    strItem++;

  }

  // update the db 
  try {
    m_dbSvc->resolveStreams(strIds, vids);
  } catch (castor::exception::Exception e){
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 19, 0, NULL);

    tapeItem=tapesUsed.begin();
    while (tapeItem!=tapesUsed.end()) {
      // release the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID",(*tapeItem)->vid())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 20, 1, params);
      try {
	vmgrHelper.resetBusyTape(**tapeItem);
      } catch (castor::exception::Exception e){
	// log TODO
	
      }
      tapeItem++;
    }
  }

  // cleanUp

  tapeItem=tapesUsed.begin();
  while (tapeItem!=tapesUsed.end()) {
      if (*tapeItem) delete *tapeItem;
      *tapeItem=NULL;
      tapeItem++;
  }
  tapesUsed.clear();

  strItem=streamsToResolve.begin();
  while (strItem !=streamsToResolve.end() ){
    if (*strItem) delete *strItem;
    *strItem=NULL;
    strItem++;
  }
  streamsToResolve.clear();
  strIds.clear(); 
  vids.clear();
  
}

