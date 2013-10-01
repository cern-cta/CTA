/******************************************************************************
 *                 castor/tape/mediachanger/AcsCmd.hpp
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

#ifndef CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP
#define CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP 1

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/mediachanger/Acs.hpp"

#include <istream>
#include <ostream>
#include <string>

namespace castor {
namespace tape {
namespace mediachanger {

/**
 * Abstract class implementing common code and data structures for command-line
 * tools that interact with ACLS compatible tape libraries.
 */
class AcsCmd {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param acs Wrapper around the ACSLS C-API.
   */
  AcsCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, Acs &acs) throw();

  /**
   * Pure-virtual destructor to guarantee this class is abstract.
   */
  virtual ~AcsCmd() throw() = 0;

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
   * Wrapper around the ACSLS C-API.
   */
  Acs &m_acs;
}; // class AcsCmd

} // namespace mediachanger
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_MEDIACHANGER_ACSCMD_HPP
