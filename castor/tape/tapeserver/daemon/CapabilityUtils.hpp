/******************************************************************************
 *         castor/tape/tapeserver/daemon/CapabilityUtils.hpp
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

#include <string>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Abstract class that defines the interface to a utility class providing
 * support for UNIX capabilities.
 *
 * The purpose of this abstract class is to faciliate unit testing.  Changing
 * the capabilities of a process requires priviledges that a unit-test
 * program will probably not have.  Developers of unit tests can derive dummy
 * utility classes from this abstract class that do not actual change the
 * capbilities of a process.
 */
class CapabilityUtils {
public:

  /**
   * Destructor.
   */
  virtual ~CapabilityUtils() throw() = 0;

  /**
   * C++ wrapper around the C functions cap_get_proc() and cap_to_text().
   */
  virtual std::string capGetProcText() = 0;

  /**
   * C++ wrapper around the C functions cap_from_text() and cap_set_proc().
   */
  virtual void capSetProcText(const std::string &text) = 0;

}; // class CapabilityUtils

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

