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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class wrapping the enumeration of the different types of message that can be
 * sent between the ProcessForkerProxy and the ProcessForker.
 */
class ProcessForkerMsgType {
public:

  /**
   * Enumeration of the different types of message that can be sent between the
   * ProcessForkerProxy and the ProcessForker.
   */
  enum Enum {
    MSG_NONE,
    MSG_FORKCLEANER,
    MSG_FORKDATATRANSFER,
    MSG_FORKLABEL,
    MSG_FORKSUCCEEDED,
    MSG_STATUS,
    MSG_STOPPROCESSFORKER
  };

  /**
   * Returns the string representation of the specified enumeration value.
   *
   * This method doe snot throw an exception.  If the speciied enumeration value
   * is not known then an appropraie string shall be returned.
   *
   * This method is thread safe.  All values returns are pointers to string
   * literals.
   *
   * @param value The enumeration value.
   */
  static const char *toString(const Enum value) throw();

}; // class ProcessForkerProxyType

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
