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
 * @(#)$RCSfile: VmgrTapeGatewayHelper.cpp,v $ $Revision: 1.15 $ $Release$ 
 * $Date: 2009/08/13 16:34:56 $ $Author: gtaur $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <errno.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>
#include <cstring>
#include <climits>

#include "vmgr_api.h"
#include "castor/Services.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"
#include "castor/stager/TapeTpModeCodes.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

void castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream(
    const u_signed64 initialSizeToTransfer,
    const std::string& tapepoolName,
    int& startFseq,castor:: stager::Tape& tapeToUse,
    const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception){
  // Sanity checks
  if ( tapepoolName.empty() || initialSizeToTransfer==0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
    << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
    << " invalid input";
    throw ex;
  }
  // call to vmgr
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
  int rc = vmgr_gettape(tapepoolName.c_str(), 
                        initialSizeToTransfer,
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
    ex.getMessage() <<
      "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
      " vmgr_gettape failed"
      ": poolname='" << tapepoolName << "'"
      " size=" << initialSizeToTransfer;
    throw ex;
  }
  tapeToUse.setVid(vid);
  tapeToUse.setSide(side);
  tapeToUse.setTpmode(castor::stager::TPMODE_WRITE);
  
  //Check that the returned start fseq is OK.
  if (label == NULL) {
    resetBusyTape(tapeToUse, shuttingDown);
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
      << " invalid label";
    throw ex;
  }
  int maxPossible=maxFseqFromLabel(label);
  if ( maxPossible > 0 && startFseq >  maxPossible ) {
    // too big fseq 
    serrno=0;
    rc = vmgr_updatetape(
			 vid,
			 side,
			 (u_signed64) 0,
			 0,
			 0,
			 TAPE_RDONLY
			 );
    if (rc<0) {
      resetBusyTape(tapeToUse, shuttingDown);
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
	<< " vmgr_updatetape failed when updating due to too big fseq: " << startFseq
        << ". For label type \"" << label << "\" maximum is "
        << maxPossible;
      throw ex;
    }
    castor::exception::Exception ex(ERTMAXERR);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForStream"
      << " too big fseq: " << startFseq
      << ". For label type \"" << label << "\" maximum is "
      << maxPossible;
    throw ex;
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::getDataFromVmgr(castor::stager::Tape& tape,
    const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception){
  TapeInfoAssertAvailable tinfo(tape, shuttingDown); /* Retrieve from vmgr through helper class, with assertion */
  tape.setDgn(tinfo.dgnBuffer);
  tape.setLabel(tinfo.vmgrTapeInfo.lbltype);
  tape.setDensity(tinfo.vmgrTapeInfo.density);
}

void  castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape(const castor::stager::Tape& tape,
    const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception)
{
  // in case of error we free the busy tape to be picked by someone else
  if ( tape.tpmode() == castor::stager::TPMODE_READ ) return; // read: don't reset it
  // Make sure we don't override some important status already set
  int status=0;
  TapeInfo tinfo(tape, shuttingDown); /* Retrieve from vmgr through helper class */
  status = tinfo.vmgrTapeInfo.status;
  if (!(status & TAPE_BUSY)) return;
  status = status & ~TAPE_BUSY;
  serrno=0;
  int rc= vmgr_modifytape(tape.vid().c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status); 
  if (rc<0){ 
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape"
      << " vmgr_modifytape failed";
    throw ex;
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr(
    const castor::tape::tapegateway::FileMigratedNotification& file,
    const std::string& vid, const utils::BoolFunctor &shuttingDown)
throw (castor::exception::Exception){
  castor::stager::Tape tape;
  tape.setVid(vid);
  tape.setSide(0); // HARDCODED
  TapeInfoAssertAvailable tinfo(tape, shuttingDown); /* Retrieve from vmgr through helper class, with assertion */
  int flags = tinfo.vmgrTapeInfo.status;

  // check if the fseq is not too big, in this case we mark it as error.
  int fseq= file.fseq();
  int maxFseq = maxFseqFromLabel(tinfo.vmgrTapeInfo.lbltype);
  //TODO new type
  if (maxFseq <= 0 || fseq >= maxFseq) {
    // reset status to RDONLY
    flags = TAPE_RDONLY;
    int rc = vmgr_updatetape(tape.vid().c_str(), tape.side(), file.fileSize(), 100, 0, flags );
    if (rc<0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
           << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
           << " vmgr_updatetape failed after invalid fseq: " << fseq
           << ". For label type \"" << tinfo.vmgrTapeInfo.lbltype << "\" maximum is "
           << maxFseq;
      throw ex;
    }
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
         << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
         <<" invalid fseq";
    throw ex;
  }
  // File migrated correctly, update vmgr and continue to migrate from the tape
  serrno=0;
  u_signed64 compression = (file.fileSize() * 100 )/file.compressedFileSize()  ;
  int rc = vmgr_updatetape(tape.vid().c_str(), tape.side(), file.fileSize(),
      compression, 1, flags ); // number files always one
  if (rc <0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
           << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
           <<" vmgr_updatetape failed";
    throw ex;
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsFull(const castor::stager::Tape& tape,
    const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception){
  // called if FileErrorReport ENOSPC

  int status=0;
  TapeInfo tinfo(tape, shuttingDown); /* Retrieve from vmgr through helper class */
  status = tinfo.vmgrTapeInfo.status;
  if ( (status & (DISABLED|EXPORTED|TAPE_RDONLY|ARCHIVED)) == 0 ) {
    status = TAPE_FULL;
    serrno=0;
    int rc= vmgr_modifytape(tape.vid().c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status); 
    if (rc <0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsFull"
	<<" vmgr_modifytape failed with rc=" << rc << " serrno=" << serrno;
      throw ex;
    }
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsReadonlyAndUnbusy(const castor::stager::Tape& tape,
    const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception)
{
  int status=0;
  TapeInfo tinfo(tape, shuttingDown); /* Retrieve from vmgr through helper class */
  status = tinfo.vmgrTapeInfo.status;
  // Set the readonly bit in status
  status |= TAPE_RDONLY;
  status &= ~TAPE_BUSY;
  serrno=0;
  int rc= vmgr_modifytape(tape.vid().c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status);
  if (rc <0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
            << "castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsReadonly"
            <<" vmgr_modifytape failed with rc=" << rc << " serrno=" << serrno;
    throw ex;
  }
}

int castor::tape::tapegateway::VmgrTapeGatewayHelper::maxFseqFromLabel(const char* label)
{
  int maxPossible = 0;
  if ((strcmp(label,"al") == 0) ||   /* Ansi Label */
       (strcmp(label,"sl") == 0))    /* Standard Label */
    maxPossible = 9999;
   if (strcmp(label,"aul") == 0)     /* Ansi (extended) User Label */
     maxPossible = INT_MAX / 3;      /* TODO XXX This is an architecture-dependent maximum... Suspect... */
   if ((strcmp(label,"nl") == 0) ||  /* No Label */
       (strcmp(label,"blp") == 0))   /* Bypass Label Type */
     maxPossible = INT_MAX;
   return maxPossible;
}

castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo::TapeInfo(const castor::stager::Tape& tape,
    const utils::BoolFunctor &shuttingDown)
throw (castor::exception::Exception)
{
  /* Initialise members to 0 */
  memset(&vmgrTapeInfo, 0, sizeof(vmgrTapeInfo));
  memset(dgnBuffer, 0, sizeof(dgnBuffer));
  /* Keep track of VID, side */
  m_vid = tape.vid();
  m_side = tape.side();
  /* Sanity check */
  if ( m_vid.empty()) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo: empty vid";
    throw ex;
  }
  // Loop on some types of vmgr errors, fail for the rest of them.
  while(1) {
    int save_serrno = 0;
    int rc;
    serrno = 0;
    rc = vmgr_querytape_byte_u64(m_vid.c_str(), m_side, &vmgrTapeInfo, dgnBuffer);
    save_serrno = serrno;
    if (!rc) return; // If rc == 0, we're done, else we have to do more checks
    // go on looping if adequate, of fail.
    if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
      if (shuttingDown()) {
        castor::exception::Exception ex(ESHUTDOWN);
        ex.getMessage()
                << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo"
                << " Cannot retry: shutting down.";
        throw ex;
      }
      castor::dlf::Param params[] = {
          castor::dlf::Param("VID",  m_vid),
          castor::dlf::Param("serrno",save_serrno)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, VMGR_GATEWAY_HELPER_RETRYING, params);
      sleep(5);
      /* Clean up structures for next call to vmgr */
      memset(&vmgrTapeInfo, 0, sizeof(vmgrTapeInfo));
      memset(dgnBuffer, 0, sizeof(dgnBuffer));
    } else {
      castor::exception::Exception ex(save_serrno);
      ex.getMessage()
              << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo"
              << " vmgr_querytape failed";
      throw ex;
    }
  }
}

castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfoAssertAvailable::TapeInfoAssertAvailable(
    const castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
throw (castor::exception::Exception): TapeInfo(tape, shuttingDown)
{
  // Interpret the status and throw an exception for non-available tapes.
  int err_number;
  std::string statName;
  if (vmgrTapeInfo.status & (DISABLED|EXPORTED|ARCHIVED)){
    if (vmgrTapeInfo.status & DISABLED ) {
      err_number = ETHELD;
      statName = "DISABLED";
    } else if ( vmgrTapeInfo.status & EXPORTED ) {
      err_number = ETABSENT;
      statName = "EXPORTED";
    } else if ( vmgrTapeInfo.status & ARCHIVED) {
      err_number = ETARCH;
      statName = "ARCHIVED";
    }
    castor::exception::Exception ex(err_number);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeStatusInVmgr"
      << " tape is not available: " << statName;
    throw ex;
  }
}
