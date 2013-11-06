/******************************************************************************
 *                      castor/tape/tapebridge/SessionErrorList.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/dlf/Dlf.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/SessionErrorList.hpp"
#include "castor/tape/utils/utils.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::SessionErrorList::SessionErrorList(
  const Cuuid_t                       &cuuid,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  const tapegateway::Volume           &volume) throw():
  m_cuuid(cuuid),
  m_jobRequest(jobRequest),
  m_volume(volume) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// push_back
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionErrorList::push_back(
  const SessionError &error) {

  std::list<SessionError>::push_back(error);

  // Log the session-error to DLF if it was the first one to be pushed onto the
  // back of the list of session-errors
  if(1 == size()) {
    logSessionError(error);
  }
}


//-----------------------------------------------------------------------------
// logSessionError
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionErrorList::logSessionError(
  const SessionError &error) throw() {
  if(SessionError::FILE_SCOPE == error.getErrorScope()) {
    logFileScopeSessionError(error);
  } else {
    logNonFileScopeSessionError(error);
  }
}


//-----------------------------------------------------------------------------
// logFileScopeSessionError
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionErrorList::logFileScopeSessionError(
  const SessionError &error) throw() {
  try {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId       ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId       ),
      castor::dlf::Param("TPVID"             , m_volume.vid()              ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit      ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn            ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost     ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort     ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorScope"        ,
        SessionError::errorScopeToString(error.getErrorScope())            ),
      castor::dlf::Param("errorCode"         , error.getErrorCode()        ),
      castor::dlf::Param("errorMessage"      , error.getErrorMessage()     ),
      castor::dlf::Param("fileTransactionId" , error.getFileTransactionId()),
      castor::dlf::Param("nsHost"            , error.getNsHost()           ),
      castor::dlf::Param("nsFileId"          , error.getNsFileId()         ),
      castor::dlf::Param("tapeFSeq"          , error.getTapeFSeq()         )};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_FIRST_SESSION_ERROR, params);
  } catch(...) {
    // Do nothing
  }
}


//-----------------------------------------------------------------------------
// logNonFileScopeSessionError
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionErrorList::logNonFileScopeSessionError(
  const SessionError &error) throw() {
  try {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId       ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId       ),
      castor::dlf::Param("TPVID"             , m_volume.vid()              ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit      ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn            ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost     ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort     ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorScope"        ,
        SessionError::errorScopeToString(error.getErrorScope())            ),
      castor::dlf::Param("errorCode"         , error.getErrorCode()        ),
      castor::dlf::Param("errorMessage"      , error.getErrorMessage()     )};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_FIRST_SESSION_ERROR, params);
  } catch(...) {
    // Do nothing
  }
}
