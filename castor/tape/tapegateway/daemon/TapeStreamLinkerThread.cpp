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


#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/stager/TapePool.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/TapeStreamLinkerThread.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeStreamLinkerThread::TapeStreamLinkerThread(){

}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::TapeStreamLinkerThread::run(void*)
{

  std::list<castor::stager::Stream> streamsToResolve;
  std::list<castor::stager::TapePool> tapepools;
  

 // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);


  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }

  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);

  // get streams to check from the db
  try {
     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_GETTING_STREAMS, 0, NULL);
     oraSvc->getStreamsWithoutTapes(streamsToResolve, tapepools);

  } catch (castor::exception::Exception& e) {
    // error in getting new tape to submit
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NO_STREAM, params);
    return;
  }

  if (streamsToResolve.empty()){
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_NO_STREAM, 0, NULL);
    return;
  }
  
  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsTapes[] =
    {
     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_STREAMS_FOUND, paramsTapes);

  if (streamsToResolve.size() !=  tapepools.size()){

    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::TapeStreamLinkerThread"
      << " invalid input";
    throw ex;

  }

  // ask tapes to vmgr


  VmgrTapeGatewayHelper vmgrHelper;


  std::list<u_signed64> strIds;
  std::list<std::string> vids;
  std::list<int> fseqs;
  std::list<castor::stager::Tape> tapesUsed;

  std::list<castor::stager::TapePool>::iterator tapepool=tapepools.begin();

  for (std::list<castor::stager::Stream>::iterator strItem=streamsToResolve.begin();
       strItem != streamsToResolve.end();
       strItem++,
       tapepool++){

    // get tape from vmgr


    castor::dlf::Param paramsVmgr[] =
      {castor::dlf::Param("StreamId",(*strItem).id()),
       castor::dlf::Param("TapePool",(*tapepool).name())
      };
    

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,LINKER_QUERYING_VMGR, paramsVmgr);
    int lastFseq=-1;
    castor::stager::Tape tapeToUse;
    
    try {
      // last Fseq is the value which should be used for the first file
      vmgrHelper.getTapeForStream(*strItem,*tapepool,lastFseq,tapeToUse);
    } catch(castor::exception::Exception& e) {
      castor::dlf::Param params[] =
	{castor::dlf::Param("StreamId",(*strItem).id()),
	 castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, LINKER_NO_TAPE_AVAILABLE, params);

      // different errors from vmgr
      
      if (e.code()== ENOENT){
	
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NOT_POOL, params);
	//tapepool doesn't exists anymore

	try {
	  
	  oraSvc->deleteStreamWithBadTapePool(*strItem);

	} catch (castor::exception::Exception& e){
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);
	}
      }

      continue;
      // in case of errors we don't change the status from TO_BE_RESOLVED to TO_BE_SENT_TO_VDQM -- NO NEED OF WAITSPACE status
    }

    if ( !tapeToUse.vid().empty()){
      // got the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("StreamId",(*strItem).id()),
	 castor::dlf::Param("TPVID",tapeToUse.vid())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_LINKING_TAPE_STREAM, params);

      strIds.push_back((*strItem).id());
      vids.push_back(tapeToUse.vid());
      fseqs.push_back(lastFseq);
      tapesUsed.push_back(tapeToUse);

    }

  }

  gettimeofday(&tvStart, NULL);

  // update the db
  try {
    oraSvc->attachTapesToStreams(strIds, vids, fseqs);
  } catch (castor::exception::Exception& e){

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);

    for (std::list<castor::stager::Tape>::iterator tapeItem=tapesUsed.begin();
	 tapeItem!=tapesUsed.end();
	 tapeItem++) {
      // release the tape
      castor::dlf::Param params[] =
	{castor::dlf::Param("TPVID",(*tapeItem).vid())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_RELEASED_BUSY_TAPE, params);
      try {
	vmgrHelper.resetBusyTape(*tapeItem);
      } catch (castor::exception::Exception& e){
	
	castor::dlf::Param params[] =
	  {castor::dlf::Param("TPVID",(*tapeItem).vid()),
	   castor::dlf::Param("errorCode",sstrerror(e.code())),
	   castor::dlf::Param("errorMessage",e.getMessage().str())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);
      }
    }
  }

  gettimeofday(&tvEnd, NULL);
  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsAttached[] =
    {
     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_TAPES_ATTACHED, paramsAttached);


}

