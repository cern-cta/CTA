/******************************************************************************
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include <string>

namespace castor {
namespace tape   {
namespace utils  {

/**
 * The data stored in a data-line (as opposed to a comment-line) from a
 * TPCONFIG file (/etc/castor/TPCONFIG).
 */
struct TpconfigLine {
  std::string unitName;
  std::string dgn;
  std::string devFilename;
  std::string density;
  std::string librarySlot;
  std::string devType;

  /**
   * Constructor.
   */
  TpconfigLine(
    const std::string &unitName,
    const std::string &dgn,
    const std::string &devFilename,
    const std::string &density,
    const std::string &librarySlot,
    const std::string &devType) throw();
}; // struct TpconfigLine

} // namespace utils
} // namespace tape
} // namespace castor

