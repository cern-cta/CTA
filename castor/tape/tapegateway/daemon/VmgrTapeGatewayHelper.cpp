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

//------------------------------------------------------------------------------
// getTapeForMigration
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration
(const u_signed64 initialSizeToTransfer,
 const std::string& tapepoolName,
 std::string &outVid,
 int& outStartFseq,
 const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  // Sanity checks
  if (tapepoolName.empty() || initialSizeToTransfer==0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration"
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
  // note that side is ignored, but is needed to please the vmgr API
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
                        &outStartFseq,
                        &estimatedFreeSpace
                        );
  if (rc < 0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration"
                    << " vmgr_gettape failed: poolname='" << tapepoolName
                    << "' size=" << initialSizeToTransfer;
    throw ex;
  }
  outVid = vid;
  
  //Check that the returned start fseq is OK.
  if (label[0] == '\0') {
    resetBusyTape(vid, shuttingDown);
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration"
                    << " invalid label";
    throw ex;
  }
  const signed64 maxPossible=maxFseqFromLabel(label);
  if (maxPossible > 0 && outStartFseq > maxPossible) {
    // too big fseq
    serrno=0;
    rc = vmgr_updatetape(vid,
			 side,
			 (u_signed64) 0,
			 0,
			 0,
			 TAPE_RDONLY);
    if (rc < 0) {
      resetBusyTape(outVid, shuttingDown);
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration"
	<< " vmgr_updatetape failed when updating due to too big fseq: " << outStartFseq
        << ". For label type \"" << label << "\" maximum is "
        << maxPossible;
      throw ex;
    }
    castor::exception::Exception ex(ERTMAXERR);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeForMigration"
      << " too big fseq: " << outStartFseq
      << ". For label type \"" << label << "\" maximum is "
      << maxPossible;
    throw ex;
  }
}

void  castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape
(const std::string &vid, const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  /* Retrieve from vmgr through helper class */
  TapeInfo info = getTapeInfo(vid, shuttingDown);
  int status = info.vmgrTapeInfo.status;
  // Make sure we don't override some important status already set
  if (!(status & TAPE_BUSY)) return;
  status = status & ~TAPE_BUSY;
  serrno=0;
  int rc= vmgr_modifytape(vid.c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status); 
  if (rc<0){ 
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VmgrTapeGatewayHelper::resetBusyTape"
      << " vmgr_modifytape failed";
    throw ex;
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::bulkUpdateTapeInVmgr(
    u_signed64 filesCount, signed64 highestFseq, u_signed64 totalBytes,
    u_signed64 totalCompressedBytes, const std::string& vid,
    const utils::BoolFunctor &shuttingDown)
throw (castor::exception::Exception){
  TapeInfo tinfo = getTapeInfoAssertAvailable (vid, shuttingDown); /* Retrieve from vmgr through helper class, with assertion */
  int flags = tinfo.vmgrTapeInfo.status;

  // Prepare sanity checks
  const int maxFseq = maxFseqFromLabel(tinfo.vmgrTapeInfo.lbltype);
  std::stringstream problemDescription;
  bool abortUpdate = false;
  // check if the highestFseq is not too big, in this case we mark it as error.
  if (maxFseq <= 0 || highestFseq >= maxFseq) {
    problemDescription << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
                       << " invalid fseq: " << highestFseq << " where maximum is "
                       << maxFseq << " for label " << tinfo.vmgrTapeInfo.lbltype;
    abortUpdate = true;
  // Check that we will actually reach the highestFseq with the current filesCount
  // fail if not.
  } else if ((int)(tinfo.vmgrTapeInfo.nbfiles + filesCount) != highestFseq) {
    problemDescription << "castor::tape::tapegateway::VmgrTapeGatewayHelper::updateTapeInVmgr"
                       << " invalid initial fseq before update: fseq=" << highestFseq << " would not be reached "
                       << "when adding count=" <<  filesCount << " to initial fseq=" << tinfo.vmgrTapeInfo.nbfiles;
    abortUpdate = true;
  }

  if (abortUpdate) {
    // We have a no-go: reset status to RDONLY
    flags = TAPE_RDONLY;
    int rc = vmgr_updatetape(vid.c_str(), /* Hardcoded side */ 0, /* Zero file size */ 0, 100, 0, flags );
    if (rc<0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
           << "castor::tape::tapegateway::VmgrTapeGatewayHelper::bulkUpdateTapeInVmgr"
           << " vmgr_updatetape failed when setting tape readonly following: "
           << problemDescription.str();
      throw ex;
    }
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << problemDescription.str();
    throw ex;
  }

  // Data sanity OK, update vmgr and continue to migrate from the tape
  serrno = 0;
  u_signed64 compression = (totalBytes * 100)/totalCompressedBytes;
  int rc = vmgr_updatetape(vid.c_str(), /* Hardcoded side */ 0, totalBytes,
      compression, filesCount, flags ); // number files always one
  if (rc <0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
           << "castor::tape::tapegateway::VmgrTapeGatewayHelper::bulkUpdateTapeInVmgr:"
           << " vmgr_updatetape failed with vid=" << vid <<  "side=0 totalBytes="
           << totalBytes << " compression=" << compression << " filesCount="
           << filesCount << " flags=0x" << std::hex << flags << std::dec
           << "rc=" << rc;
    throw ex;
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsFull
(const std::string& vid, const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  // called if FileErrorReport ENOSPC
  /* Retrieve from vmgr through helper class */
  TapeInfo info = getTapeInfo(vid, shuttingDown);
  int status = info.vmgrTapeInfo.status;
  if ( (status & (DISABLED|EXPORTED|TAPE_RDONLY|ARCHIVED)) == 0 ) {
    status = TAPE_FULL;
    serrno=0;
    int rc= vmgr_modifytape(vid.c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status); 
    if (rc <0) {
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsFull"
	<<" vmgr_modifytape failed with rc=" << rc << " serrno=" << serrno;
      throw ex;
    }
  }
}

void castor::tape::tapegateway::VmgrTapeGatewayHelper::setTapeAsReadonlyAndUnbusy
(const std::string &vid, const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  /* Retrieve from vmgr through helper class */
  TapeInfo info = getTapeInfo(vid, shuttingDown);
  int status = info.vmgrTapeInfo.status;
  // Set the readonly bit in status
  status |= TAPE_RDONLY;
  status &= ~TAPE_BUSY;
  serrno=0;
  int rc= vmgr_modifytape(vid.c_str(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, status);
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

//------------------------------------------------------------------------------
// getTapeInfo 
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeInfo
castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeInfo
(const std::string& vid, const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  /* Sanity check */
  if (vid.empty()) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo: empty vid";
    throw ex;
  }
  // result
  TapeInfo info;
  // Loop on some types of vmgr errors, fail for the rest of them.
  while(1) {
    int save_serrno = 0;
    int rc;
    serrno = 0;
    rc = vmgr_querytape_byte_u64(vid.c_str(), 0, &info.vmgrTapeInfo, info.dgnBuffer);  // side = 0
    save_serrno = serrno;
    if (!rc) return info; // If rc == 0, we're done, else we have to do more checks
    // go on looping if adequagte, of fail.
    if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
      if (shuttingDown()) {
        castor::exception::Exception ex(ESHUTDOWN);
        ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo"
                        << " Cannot retry: shutting down.";
        throw ex;
      }
      castor::dlf::Param params[] = {
        castor::dlf::Param("VID",  vid),
        castor::dlf::Param("serrno",save_serrno)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, VMGR_GATEWAY_HELPER_RETRYING, params);
      sleep(5);
    } else {
      castor::exception::Exception ex(save_serrno);
      ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::TapeInfo"
                      << " vmgr_querytape failed";
      throw ex;
    }
  }
  // never reached
  return info;
}

//------------------------------------------------------------------------------
// getTapeInfoAssertAvailable 
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeInfo
castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeInfoAssertAvailable
(const std::string& vid, const utils::BoolFunctor &shuttingDown)
  throw (castor::exception::Exception) {
  // call getTapeInfo
  TapeInfo info = getTapeInfo(vid, shuttingDown);
  // Interpret the status and throw an exception for non-available tapes.
  int err_number;
  std::string statName;
  if (info.vmgrTapeInfo.status & (DISABLED|EXPORTED|ARCHIVED)){
    if (info.vmgrTapeInfo.status & DISABLED) {
      err_number = ETHELD;
      statName = "DISABLED";
    } else if (info.vmgrTapeInfo.status & EXPORTED) {
      err_number = ETABSENT;
      statName = "EXPORTED";
    } else if (info.vmgrTapeInfo.status & ARCHIVED) {
      err_number = ETARCH;
      statName = "ARCHIVED";
    }
    castor::exception::Exception ex(err_number);
    ex.getMessage() << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getTapeStatusInVmgr"
                    << " tape is not available: " << statName;
    throw ex;
  }
  return info;
}
