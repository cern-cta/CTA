/******************************************************************************
 *                 castor/tape/tpcp/Verifier.cpp
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
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/Verifier.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Verifier::Verifier(ParsedCommandLine &cmdLine,
  FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
  const char *const dgn, const int volReqId,
  castor::io::ServerSocket &callbackSock) throw() :
  ActionHandler(cmdLine, filenames, vmgrTapeInfo, dgn, volReqId, callbackSock) {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::Verifier::run() throw(castor::exception::Exception) {

  castor::exception::Exception ex(ECANCELED);

  ex.getMessage() << "Verifier not implemented";

  throw ex;
}
