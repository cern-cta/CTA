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

#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/DebugBuf.hpp"

#include <istream>
#include <ostream>
#include <string>

namespace cta {
namespace mediachanger {

/**
 * Abstract class implementing common code and data structures for a
 * command-line tool.
 */
class CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param mc Interface to the media changer.
   */
  CmdLineTool(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, MediaChangerFacade &mc)
   ;

  /**
   * Pure-virtual destructor to guarantee this class is abstract.
   */
  virtual ~CmdLineTool() = 0;

protected:

  /**
   * Standard input stream.
   */
  std::istream &m_in;

  /**
   * Standard output stream.
   */
  std::ostream &m_out;

  /**
   * Standard error stream.
   */
  std::ostream &m_err;

  /**
   * Interface to the media changer.
   */
  MediaChangerFacade &m_mc;

  /**
   * Debug stream buffer that inserts a standard debug preamble before each
   * message-line written to it.
   */
  DebugBuf m_debugBuf;

  /**
   * Stream used to write debug messages.
   *
   * This stream will insert a standard debug preamble before each message-line
   * written to it.
   */
  std::ostream m_dbg;

  /**
   * Returns the string representation of the specfied boolean value.
   *
   * @param value The boolean value.
   */
  std::string bool2Str(const bool value) const;

}; // class CmdLineTool

} // namespace mediachanger
} // namespace cta
