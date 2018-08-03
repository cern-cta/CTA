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
