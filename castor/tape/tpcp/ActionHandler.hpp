/******************************************************************************
 *                 castor/tape/tpcp/ActionHandler.hpp
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

#ifndef CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP
#define CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "h/Castor_limits.h"
#include "h/vmgr_api.h"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Asbtract class specifying the run() method to be implemented by all Action
 * handlers.
 */
class ActionHandler {
public:

  /**
   * Constructor.
   *
   * @param debug True if debug messages should be displayed.
   * @param tapeFseqRanges The list of tape file sequence ranges to be
   * processed.
   * @param filenames The list of RFIO filenames to be processed.
   * @param vmgrTapeInfo The information retrieved from the VMGR about the tape
   * to be used.
   * @param dgn The DGN of the tape to be worked on.
   * @param volReqId The volume request ID returned by the VDQM in response to
   * the request for a drive.
   * @param callbackSocket The aggregator callback socket.
   */
  ActionHandler(const bool debug, TapeFseqRangeList &tapeFseqRanges,
    FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
    const char *const dgn, const int volReqId,
    castor::io::ServerSocket &callbackSocket) throw();

  /**
   * Performs the action.
   */
  virtual void run() throw(castor::exception::Exception) = 0;


protected:

  /**
   * True if debug messages should be displayed.
   */
  const bool m_debug;

  /**
   * The list of tape file sequence ranges to be processed.
   */
  TapeFseqRangeList &m_tapeFseqRanges;

  /**
   * The list of RFIO filenames to be processed.
   */
  FilenameList &m_filenames;

  /**
   *  The information retrieved from the VMGR about the tape to be used.
   */
  const vmgr_tape_info &m_vmgrTapeInfo;

  /**
   * The DGN of the tape to be worked on.
   */
  const char *const m_dgn;

  /**
   * The volume request ID returned by the VDQM in response to the request for
   * a drive.
   */
  const int m_volReqId;

  /**
   * The aggregator callback socket.
   */
  castor::io::ServerSocket &m_callbackSocket;

  /**
   * Acknowledges the end of the session to the aggregator.
   */
  void acknowledgeEndOfSession() throw(castor::exception::Exception);

}; // class ActionHandler

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP
