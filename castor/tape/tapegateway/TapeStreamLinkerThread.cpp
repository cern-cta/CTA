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
 * @(#)$RCSfile: TapeStreamLinkerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/TapePool.hpp"

#include "castor/tape/tapegateway/DlfCodes.hpp"
#include "castor/tape/tapegateway/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/TapeStreamLinkerThread.hpp"
#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeStreamLinkerThread::TapeStreamLinkerThread(){

}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::TapeStreamLinkerThread::run(void* par)
{

  std::vector<castor::stager::Stream*> streamsToResolve;
  std::vector<castor::stager::Stream*>::iterator stream;

 // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);


  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }


  // get streams to check from the db
  try {
     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_GETTING_STREAMS, 0, NULL);
    streamsToResolve= oraSvc->getStreamsWithoutTapes();
  } catch (castor::exception::Exception e) {
    // error in getting new tape to submit
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NO_STREAM, 2, params);
    return;
  }

  // ask tapes to vmgr

  std::vector<u_signed64> strIds;
  std::vector<std::string> vids;
  std::vector<int> fseqs;
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
    int lastFseq=-1;
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,LINKER_QUERYING_VMGR, 1, params0);
    try {
      // last Fseq is the value which should be used for the first file
      tapeToUse=vmgrHelper.getTapeForStream(**strItem,lastFseq);
    } catch(castor::exception::Exception e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("StreamId",(*strItem)->id()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_NO_TAPE_AVAILABLE, 3, params);
      strItem++;
      continue;
      // in case of errors we don't change the status from TO_BE_RESOLVED to TO_BE_SENT_TO_VDQM -- NO NEED OF WAITSPACE status
    }

    if ( tapeToUse != NULL ){
      // got the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("StreamId",(*strItem)->id()),
	 castor::dlf::Param("TPVID",tapeToUse->vid())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_LINKING_TAPE_STREAM, 2, params);

      strIds.push_back((*strItem)->id());
      vids.push_back(tapeToUse->vid());
      fseqs.push_back(lastFseq);
      tapesUsed.push_back(tapeToUse);

    }

    strItem++;

  }

  // update the db
  try {
    oraSvc->attachTapesToStreams(strIds, vids, fseqs);
  } catch (castor::exception::Exception e){

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, 2, params);

    tapeItem=tapesUsed.begin();
    while (tapeItem!=tapesUsed.end()) {
      // release the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("TPVID",(*tapeItem)->vid())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_RELEASED_BUSY_TAPE, 1, params);
      try {
	vmgrHelper.resetBusyTape(**tapeItem);
      } catch (castor::exception::Exception e){

	castor::dlf::Param params[] =
	  {castor::dlf::Param("TPVID",(*tapeItem)->vid()),
	   castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, 3, params);
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
  fseqs.clear();

}

