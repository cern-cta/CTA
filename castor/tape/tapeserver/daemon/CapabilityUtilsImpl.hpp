/******************************************************************************
 *         castor/tape/tapeserver/daemon/CapabilityUtilsImpl.hpp
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

#include "castor/tape/tapeserver/daemon/CapabilityUtils.hpp"

#include <string>
#include <sys/capability.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Concrete implementation of a utility class providing support for UNIX
 * capabilities.
 */
class CapabilityUtilsImpl: public CapabilityUtils {
public:

  /**
   * Destructor.
   */
  ~CapabilityUtilsImpl() throw();

  /**
   * C++ wrapper around the C functions cap_get_proc() and cap_to_text().
   */
  std::string capGetProcText();

  /**
   * C++ wrapper around the C functions cap_from_text() and cap_set_proc().
   */
  void capSetProcText(const std::string &text);

private:

  /**
   * C++ wrapper around the C function cap_get_proc().
   */
  cap_t capGetProc();

  /**
   * C++ wrapper around the C function cap_to_text().
   */
  std::string capToText(const cap_t cap);

  /**
   * C++ wrapper around the C function cap_from_text().
   */
  cap_t capFromText(const std::string &txt);

  /**
   * C++ wrapper around the C function cape_set_proc().
   */
  void capSetProc(const cap_t cap);

}; // class CapabilityUtils

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
