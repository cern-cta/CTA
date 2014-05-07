/******************************************************************************
 *                 castor/tape/label/LabelCmd.hpp
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

#pragma once

#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/tape/rmc/DebugBuf.hpp"

#include <istream>
#include <ostream>
#include <string>

namespace castor {
namespace tape {
namespace label {

/**
 * Class implementing the business logic of the label command-line tool.
 */
class LabelCmd {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param tapeserver Proxy object representing the tapeserverd daemon.
   */
  LabelCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, legacymsg::TapeserverProxy &tapeserver) throw();

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
   * Proxy object representing the tapeserverd daemon.
   */
  legacymsg::TapeserverProxy &m_tapeserver

  /**
   * Returns the string representation of the specfied boolean value.
   *
   * @param value The boolean value.
   */
  std::string bool2Str(const bool value) const throw();

}; // class LabelCmd

} // namespace label
} // namespace tape
} // namespace castor
