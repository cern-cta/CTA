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
   * Performs the action as laid out in the specified parsed command-line
   * arguments.
   *
   * @param debug True if debug messages should be displayed.
   * @param action The Action to be performed.
   * @param vid The VID of the tape to be worked on.
   * @param tapeFseqRanges The list of tape file sequence ranges to be
   * processed.
   * @param filenames The list of RFIO filenames to be processed.
   * @param dgn The DGN of the tape to be worked on.
   * @param volReqId The volume request ID returned by the VDQM in response to
   * to the request for a drive.
   * @param callbackSocket The aggregator callback socket.
   */
  virtual void run(bool debug, Action &action, char (&vid)[CA_MAXVIDLEN+1],
    TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
    const char *dgn, const int volReqId,
    castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception) = 0;

}; // class ActionHandler

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP
