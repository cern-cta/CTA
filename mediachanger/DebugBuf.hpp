/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
   * Constructor
   *
   * Initialises the the debug mode to be off.
   *
   * @param os The output stream to which each debug message-line togther with
   * its standard preamble shall be written.
   */
  explicit DebugBuf(std::ostream& os);

  /**
   * Destructor
   */
  ~DebugBuf() = default;

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
