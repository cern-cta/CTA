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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/NsProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A dummy rmc proxy.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class NsProxy_TapeAlwaysEmpty: public NsProxy {
public:

  /**
   * Destructor.
   */
  ~NsProxy_TapeAlwaysEmpty() throw();

  /**
   * Asks the remote nameserver daemon if the specified tape has segments (both active or disabled)
   * still registered in the nameserver DB.
   * 
   * @param  vid: Volume ID of the tape to check
   * @return 0: tape is empty. 1: tape has at least one disabled segment. 2: tape has at least one active segment
   */
  TapeNsStatus doesTapeHaveNsFiles(const std::string &vid) throw(castor::exception::Exception);

}; // class NsProxy_TapeAlwaysEmpty

} // namespace legacymsg
} // namespace castor

