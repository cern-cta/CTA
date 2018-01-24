/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "AcsCmd.hpp"
#include "AcsDismountCmdLine.hpp"
#include "common/exception/DismountFailed.hpp"
#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"

namespace cta {
namespace acs {

/**
 * The class implementing the mount command.
 */
class AcsDismountCmd: public AcsCmd {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsDismountCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs) throw();

  /**
   * Destructor.
   */
  virtual ~AcsDismountCmd() throw();

  /**
   * The entry function of the command.
   *
   * This method sets the m_cmdLine member-variable.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv);

protected:

  /**
   * Dismounts the tape with the specified VID into the drive with the specified
   * drive ID.
   *
   * This method does not return until the mount has either suceeded, failed or
   * the specified timeout has been reached.
   *
   * @param dismountTimeout The maximum amount of time in seconds to wait for
   * the dismount operation to conclude.
   * @param queryInterval The amount of time in seconds to wait between
   * querying ACS for responses.
   */
  void syncDismount();

  /**
   * Sends the dismount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendDismountRequest(const SEQ_NO seqNumber);

  /**
   * Throws cta::exception::DismountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processDismountResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]);

private:

  /**
   * The parsed command-line.
   *
   * This member-variable is set by the main() method of this class.
   */
  AcsDismountCmdLine m_cmdLine;

}; // class AcsDismountCmd

} // namespace acs
} // namespace cta
