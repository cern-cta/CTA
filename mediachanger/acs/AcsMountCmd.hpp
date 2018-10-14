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
#include "AcsMountCmdLine.hpp"
#include "common/exception/MissingOperand.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger    {
namespace acs    {

/**
 * The class implementing the mount command.
 */
class AcsMountCmd: public AcsCmd {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsMountCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs);

  /**
   * Destructor.
   */
  virtual ~AcsMountCmd();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv);

protected:

  /**
   * Mounts the tape with the specified VID into the drive with the specified
   * drive ID.
   *
   * This method does not return until the mount has either suceeded, failed or
   * the specified timeout has been reached.
   */
  void syncMount();

  /**
   * Sends the mount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendMountRequest(const SEQ_NO seqNumber);

  /**
   * Process mount rsponse.
   *
   * @param buf The mount-response message.
   */
  void processMountResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]);

private:

  /**
   * The parsed command-line.
   *
   * The value of this member variable is set within the main() method of this
   * class.
   */
  AcsMountCmdLine m_cmdLine;

}; // class AcsMountCmd

} // namespace acs
} // namespace mediachanger
} // namespace cta
