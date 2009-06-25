/******************************************************************************
 *                 castor/tape/tpcp/Verifier.hpp
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

#ifndef CASTOR_TAPE_TPCP_VERIFIER_HPP
#define CASTOR_TAPE_TPCP_VERIFIER_HPP 1

#include "castor/tape/tpcp/ActionHandler.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Responsible for carrying out the action of verifying a tape.
 */
class Verifier : public ActionHandler {
public:

  /**
   * See the header file of castor::tape::tpcp::ActionHandler for this method's
   * documentation.
   */
  void run(bool debug, TapeFseqRangeList &tapeFseqRanges,
    FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
    const char *dgn, const int volReqId,
    castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception);

}; // class Verifier

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_VERIFIER_HPP
