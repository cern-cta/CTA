
/******************************************************************************
 *                      VmgrTapeGatewayHelper.cpp
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
 * @(#)$RCSfile: VmgrTapeGatewayHelper.cpp,v $ $Revision: 1.4 $ $Release$ 
 * $Date: 2009/02/09 10:31:36 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "vmgr_api.h"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "castor/Services.hpp"
#include <u64subr.h>

#include "castor/stager/TapePool.hpp"
#include "castor/tape/tapegateway/NsFileInformation.hpp"
#include "castor/tape/tapegateway/TapeFileNsAttribute.hpp"

castor::stager::Tape* castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream(castor::stager::Stream* streamToResolve) throw (castor::exception::Exception){

  castor::stager::Tape* tape=NULL;
  if (streamToResolve == NULL || streamToResolve->tapePool() == NULL || streamToResolve->tapePool()->name().empty() || streamToResolve->initialSizeToTransfer() == 0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
    << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
    << "invalid input";
    throw ex;

  }

  // call to vmgr
  const char* tpName = streamToResolve->tapePool()->name().c_str();
  int  startFseq;
  u_signed64 estimatedFreeSpace;
  char vid[ CA_MAXVIDLEN + 1];
  *vid = '\0';
  char vsn[CA_MAXVSNLEN + 1];
  *vsn = '\0';
  char dgn[ CA_MAXDGNLEN + 1];
  *dgn = '\0';
  char density[CA_MAXDENLEN + 1];
  *density =  '\0';
  char label[CA_MAXLBLTYPLEN+1];
  *label = '\0';
  char  model[CA_MAXMODELLEN + 1];
  *model = '\0';
  int side=-1;
  
  serrno=0;
  int rc = vmgr_gettape(tpName, 
		    streamToResolve->initialSizeToTransfer(),
		    NULL,
		    vid,
		    vsn,
		    dgn,
		    density,
		    label,
		    model,
		    &side,
		    &startFseq,
		    &estimatedFreeSpace
		    );

  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
    << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
    << "vmgr_gettape failed";
    throw ex;

  }
  
  
  //Check that the returned start fseq is OK.
    
  if (label == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
      << "invalid label";
    throw ex;
    
  }
  int maxPossible=0;

  if ((strcmp(label,"al") == 0) ||  /* Ansi Label */
       (strcmp(label,"sl") == 0))    /* Standard Label */
   maxPossible = 9999;
   if (strcmp(label,"aul") == 0)     /* Ansi (extended) User Label */
   maxPossible = INT_MAX / 3;
   if ((strcmp(label,"nl") == 0) ||  /* No Label */
       (strcmp(label,"blp") == 0))   /* Bypass Label Type */
     maxPossible = INT_MAX;

  // TODO add new type
  
  if ( maxPossible > 0 && startFseq > maxPossible ) {

    // success update tape

    serrno=0;
    rc = vmgr_updatetape(
			 vid,
			 side,
			 (u_signed64) 0,
			 0,
			 0,
			 TAPE_RDONLY
			 );

    if (rc<0) return NULL;
    if (rc<0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
	<< "vmgr_updatetape failed";
      throw ex;

    }
    tape = new castor::stager::Tape();
    tape->setVid(vid);
    tape->setSide(side);
    tape->setTpmode(1); // to write
  } else {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
      << "too big fseq";
    throw ex;
  }
  return tape;
}





void castor::tape::tapegateway::VmgrTapeGatewayHelper::getDataFromVmgr(castor::stager::Tape* tape) throw (castor::exception::Exception){

  struct vmgr_tape_info vmgrTapeInfo;
  int save_serrno=0;

  if ( tape==NULL || tape->vid().empty()) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
    << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
    << "vmgr_gettape failed";
    throw ex;
  }

  // check tape

  const char* vid=tape->vid().c_str();
  int side=tape->side();
  char dgnBuffer[8];

  while(1){
    serrno=0;
    int rc = vmgr_querytape(vid,side,&vmgrTapeInfo,dgnBuffer);
    save_serrno = serrno;

    // check the status
        
    if (rc == 0) {
      // ok call vmgr
      if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
        if ( (vmgrTapeInfo.status & DISABLED) == DISABLED ) {
          serrno = ETHELD;
        } else if ( (vmgrTapeInfo.status & EXPORTED) == EXPORTED ) {
          serrno = ETABSENT;
        } else if ( (vmgrTapeInfo.status & ARCHIVED) == ARCHIVED ) {
          serrno = ETARCH;
        }
	castor::exception::Exception ex(serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
	  << "vmgr_gettape failed";
	throw ex;
        
      }
      break;
    }

    // error in querying vmgr
 
    if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
      sleep(5);
    } else {
      serrno = save_serrno;
      castor::exception::Exception ex(save_serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
	<< "vmgr_querytape failed";
	throw ex;
    }
  }
  
  tape->setDgn(dgnBuffer);
  tape->setLabel(vmgrTapeInfo.lbltype);
  tape->setDensity(vmgrTapeInfo.density);

}

int castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeStatusInVmgr(castor::stager::Tape* tape) throw (castor::exception::Exception) {

  struct vmgr_tape_info vmgrTapeInfo;
  int save_serrno = 0;

  if ( tape==NULL || tape->vid().empty()) return -1; 


  const char* vid=tape->vid().c_str();
  int side=tape->side();
  char dgnBuffer[8];

  while(1){
    serrno=0;
    int rc = vmgr_querytape(vid,side,&vmgrTapeInfo,dgnBuffer);
    save_serrno = serrno;
  
    if ( rc == 0 ) {
      if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
        if ( (vmgrTapeInfo.status & DISABLED) == DISABLED ) {
          serrno = ETHELD;
        } else if ( (vmgrTapeInfo.status & EXPORTED) == EXPORTED ) {
          serrno = ETABSENT;
        } else if ( (vmgrTapeInfo.status & ARCHIVED) == ARCHIVED ) {
          serrno = ETARCH;
        }
	
	castor::exception::Exception ex(serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
	  << "vmgr_querytape failed";
	throw ex;

      }
      return vmgrTapeInfo.status; // break and return the status 
      
    } else {
      // go on looping
      if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
	sleep(5);
      } else {
	castor::exception::Exception ex(save_serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
	  << "vmgr_querytape failed";
	throw ex;
      }
   
    }
  }

}


void  castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape(castor::stager::Tape* tape) throw (castor::exception::Exception){
  
  // in case of error we free the busy tape to be picked by someone else
 
  if ( tape->tpmode() == 1 ) return; // read don't reset it

  // Make sure we don't override some important status already set
  
  int status=getTapeStatusInVmgr(tape);
 
  if ( (status & TAPE_BUSY) == 0 ) return;
  
  status = status & ~TAPE_BUSY;
  serrno=0;
  int rc= vmgr_modifytape(tape->vid().c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status); 
  if (rc<0){
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape"
      << "vmgr_modifytape failed";
    throw ex;

  }
  
}


void castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr(castor::tape::tapegateway::FileMigratedResponse* file ) throw (castor::exception::Exception){

  int save_serrno=0;

  // check input 

  if (file == NULL ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr";
    throw ex;

  }

  int side=file->nsFileInformation()->tapeFileNsAttribute()->side(); // supposed 1 segment

  // get information from vmgr

  struct vmgr_tape_info vmgrTapeInfo;
  char dgnBuffer[8];

  while(1){
    serrno=0;
    int rc = vmgr_querytape(file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(),side,&vmgrTapeInfo,dgnBuffer);
    save_serrno = serrno;

    if ( rc == 0 ) {
      if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
        if ( (vmgrTapeInfo.status & DISABLED) == DISABLED ) {
          serrno = ETHELD;
        } else if ( (vmgrTapeInfo.status & EXPORTED) == EXPORTED ) {
          serrno = ETABSENT;
        } else if ( (vmgrTapeInfo.status & ARCHIVED) == ARCHIVED ) {
          serrno = ETARCH;
        }
	castor::exception::Exception ex(serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
	  <<"vmgr_querytape failed";
	throw ex;

      }
      break; // break and return the status 
      
    } else {
      // go on looping
      if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
	sleep(5);
      } else {
	castor::exception::Exception ex(save_serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
	  <<"vmgr_querytape failed";
	throw ex;
      }
   
    }
  }
 
 int flags = vmgrTapeInfo.status;

  // check if the fseq is not too big, in this case we mark it as error. 

 int fseq= file->nsFileInformation()->tapeFileNsAttribute()->fseq();
 int maxFseq =0;

   
 if ((strcmp(vmgrTapeInfo.lbltype,"al") == 0) ||  /* Ansi Label */
     (strcmp(vmgrTapeInfo.lbltype,"sl") == 0))    /* Standard Label */
   maxFseq=9999;
 else if (strcmp(vmgrTapeInfo.lbltype,"aul") == 0)     /* Ansi (extended) User Label */
   maxFseq=INT_MAX / 3;
 else if ((strcmp(vmgrTapeInfo.lbltype,"nl") == 0) ||  /* No Label */
	  (strcmp(vmgrTapeInfo.lbltype,"blp") == 0))   /* Bypass Label Type */
   maxFseq=INT_MAX;
 //TODO new type
 
 if (maxFseq <= 0 || fseq >= maxFseq ) { 
   // set as RDONLY
   flags = TAPE_RDONLY;
   int rc = vmgr_updatetape(file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(), side, file->nsFileInformation()->fileSize(), 100, 0, flags ); 
   if (rc<0) {
     castor::exception::Exception ex(serrno);
     ex.getMessage()
       << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
       <<"vmgr_updatetape failed after invalid fseq";
     throw ex;
    
   }
   castor::exception::Exception ex(EINVAL);
   ex.getMessage()
     << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
     <<"invalid fseq";
   throw ex;
   
 }


 // check if the tape is FULL

 if (file->errorCode() == ENOSPC){

   if ( (flags & (TAPE_FULL|DISABLED|EXPORTED|TAPE_RDONLY|ARCHIVED)) == 0 ) 
     flags = TAPE_FULL;
   serrno=0;
   int rc = vmgr_updatetape( file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(), side, file->nsFileInformation()->fileSize(), 100, 0, flags ); // no files written
   if (rc <0) {
     castor::exception::Exception ex(serrno);
     ex.getMessage()
       << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
       <<" vmgr_updatetape failed after ENOSPC";
     throw ex;
   }
   
   castor::exception::Exception ex(ENOSPC);
   ex.getMessage()
       << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
       <<" tape full";
   throw ex;
   
 }

 // File migrated correctly, update vmgr and continue to migrate from the tape
 
 serrno=0;
 int rc = vmgr_updatetape(file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(), side, file->nsFileInformation()->fileSize(),file->nsFileInformation()->tapeFileNsAttribute()->compression() , 1, flags ); // number files always one
 if (rc <0) {
   castor::exception::Exception ex(serrno);
   ex.getMessage()
       << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
       <<" vmgr_updatetape failed";
   throw ex;
 }

}


