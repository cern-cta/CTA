/******************************************************************************
 *                      NsTapeGatewayHelper.cpp
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
 * @(#)$RCSfile: NsTapeGatewayHelper.cpp,v $ $Revision: 1.15 $ $Release$ 
 * $Date: 2009/08/10 22:07:12 $ $Author: murrayc3 $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <common.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "Cns_api.h"
#include "rfio_api.h"

#include "castor/exception/OutOfMemory.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/PositionCommandCode.hpp"

#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"

// This checker function will raise an exception if the Fseq forseen for writing
// is not strictly greater than the highest know Fseq for this tape in the name
// server (meaning an overwrite).
void castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite (const std::string &vid, int Fseq)
     throw (castor::exception::Exception) {
  struct Cns_segattrs segattrs;
  memset (&segattrs, 0, sizeof(struct Cns_segattrs));
  int rc = Cns_lastfseq (vid.c_str(), 0, &segattrs); // side = 0 hardcoded
  // Read serrno only once as it points at a function hidden behind a macro.
  int save_serrno = serrno;
  // If the name server does not know about the tape, we're safe (return, done).
  if ((-1 == rc) && (ENOENT == save_serrno)) return;
  if (rc != 0) { // Failure to contact the name server and all other errors.
    castor::exception::Exception ex(save_serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite:"
      << "Cns_lastfseq failed for VID="<<vid.c_str()<< " side="<< 0 << " : rc="
      << rc << " serrno =" << save_serrno << "(" << sstrerror(save_serrno) << ")";
    throw ex;
  }
  if (Fseq <= segattrs.fseq) { // Major error, we are about to overwrite an fseq
    // referenced in the name server.
    castor::exception::Exception ex(ERTWRONGFSEQ);
    ex.getMessage()
          << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite:"
          << "Fseq check failed for VID="<<vid.c_str()<< " side="<< 0 << " Fseq="
          << Fseq << " last referenced segment in NS=" << segattrs.fseq;
    throw ex;
  }
}

