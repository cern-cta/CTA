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
#include "AcsQueryVolumeCmdLine.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger {
namespace acs {

/**
 * The class implementing the mount command.
 */
class AcsQueryVolumeCmd: public AcsCmd {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsQueryVolumeCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs);

  /**
   * Destructor.
   */
  virtual ~AcsQueryVolumeCmd();

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
   * Queries ACS for information about the volume identifier specified on the
   * command-line.
   *
   * This method does not return until the information has been successfully
   * retrieved, an error has occurred or the specified timeout has been
   * reached.
   *
   * @return The volume status of the volume identifier specified on the
   * command-line.
   */
  void syncQueryVolume();

  /**
   * Sends the query volume  request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendQueryVolumeRequest(const SEQ_NO seqNumber);

  /**
   * Extracts the volume status from the specified query-response message and
   * writes it in human-readable form to the specified output stream.
   *
   * @param os The output stream.
   * @param buf The query-response message.
   */
  void processQueryResponse(std::ostream &os,
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]);

  /**
   * Writes a human readable representation of the specified volume status to
   * the specified output stream.
   *
   * @param os The output stream.
   * @param s The volume status.
   */
  void writeVolumeStatus(std::ostream &os, const QU_VOL_STATUS &s);

private:

  /**
   * The parsed command-line.
   *
   * The value of this member variable is set within the main() method of this
   * class.
   */
  AcsQueryVolumeCmdLine m_cmdLine;

}; // class AcsQueryVolumeCmd

} // namespace acs
} // namepsace mediachanger
} // namespace cta
