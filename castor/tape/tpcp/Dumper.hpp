/******************************************************************************
 *                 castor/tape/tpcp/Dumper.hpp
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

#ifndef CASTOR_TAPE_TPCP_DUMPER_HPP
#define CASTOR_TAPE_TPCP_DUMPER_HPP 1

#include "castor/tape/tpcp/ActionHandler.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Responsible for carrying out the action of dumping a tape.
 */
class Dumper : public ActionHandler {
public:

  /**
   * Constructor calling the ActionHandler constructor.
   *
   * See the header file of castor::tape::tpcp::ActionHandler for a description
   * of the parameters.
   */
  Dumper(ParsedCommandLine &cmdLine, FilenameList &filenames,
    const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
    const int volReqId, castor::io::ServerSocket &callbackSock) throw();

  /**
   * Destructor.
   */
  virtual ~Dumper();

  /**
   * See the header file of castor::tape::tpcp::ActionHandler for this method's
   * documentation.
   */
  void run() throw(castor::exception::Exception);


private:

  /**
   * DumpNotification message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleDumpNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotification message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotificationErrorReport(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

}; // class Dumper

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_DUMPER_HPP
