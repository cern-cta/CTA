/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <ostream>
#include <streambuf>

namespace cta::mediachanger {

/**
 * Stream buffer class used to prepend a standard preamble to debug
 * message-lines.
 *
 * This stream buffer does not write any output if debug mode has not been
 * turned on by calling setDebugMode(true).  Any debug message written to this
 * stream buffer will be discarded if debug mode is off.
 */
class DebugBuf : public std::streambuf {
public:

  /**
   * Constructor.
   *
   * Initialises the the debug mode to be off.
   *
   * @param os The output stream to which each debug message-line togther with
   * its standard preamble shall be written.
   */
  DebugBuf(std::ostream &os);

  /**
   * Destructor.
   */
  ~DebugBuf();

  /**
   * Set the debug mode to be on (true) or off (false).
   *
   * The default set in the constructor is off (false).
   */
  void setDebug(const bool value);

protected:

  /**
   * Sends the specified character to the output channnel.
   */
  int_type overflow (const int_type c);

  /**
   * Writes the standard preamble to the output stream.
   */
  void writePreamble();

private:

  /**
   * True if debug mode is on.
   */
  bool m_debug;

  /**
   * The output stream to which each debug message-line togther with its
   * standard preamble shall be written.
   */
  std::ostream &m_os;

  /**
   * True is a preamble should be written.
   */
  bool m_writePreamble;

}; // class DebugBuf

} // namespace cta::mediachanger
