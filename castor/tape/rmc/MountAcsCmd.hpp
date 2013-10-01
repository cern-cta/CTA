/******************************************************************************
 *                 castor/tape/rmc/MountAcsCmd.hpp
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

#ifndef CASTOR_TAPE_MEDIACHANGER_MOUNTACSCMD_HPP
#define CASTOR_TAPE_MEDIACHANGER_MOUNTACSCMD_HPP 1

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"
#include "castor/exception/MountFailed.hpp"
#include "castor/tape/rmc/AcsCmd.hpp"
#include "castor/tape/rmc/MountAcsCmdLine.hpp"

#include <stdint.h>

namespace castor  {
namespace tape    {
namespace rmc {

/**
 * The class implementing the mount command.
 */
class MountAcsCmd: public AcsCmd {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  MountAcsCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs) throw();

  /**
   * Destructor.
   */
  virtual ~MountAcsCmd() throw();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char *const *const argv) throw();

protected:

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   * @return The parsed command-line.
   */
  MountAcsCmdLine parseCmdLine(const int argc, char *const *const argv)
    throw(castor::exception::Internal, castor::exception::InvalidArgument,
      castor::exception::MissingOperand);

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();

  /**
   * Mounts the tape with the specified VID into the drive with the specified
   * drive ID.
   *
   * This method does not return until the mount has either suceeded, failed or
   * the specified timeout has been reached.
   */
  void syncMount() throw(castor::exception::MountFailed);

  /**
   * Sends the mount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendMountRequest(const SEQ_NO seqNumber)
    throw(castor::exception::MountFailed);

  /**
   * Throws castor::exception::DismountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processMountResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
    throw(castor::exception::MountFailed);

private:

  /**
   * The parsed command-line.
   *
   * The value of this member variable is set within the main() method of this
   * class.
   */
  MountAcsCmdLine m_cmdLine;

  /**
   * The default time in seconds to wait between queries to ACS for responses.
   */
  const int m_defaultQueryInterval;

  /**
   * The default timeout value in seconds for the mount to conclude either
   * success or failure.
   */
  const int m_defaultTimeout;
}; // class MountAcsCmd

} // namespace rmc
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_MEDIACHANGER_MOUNTACSCMD_HPP
