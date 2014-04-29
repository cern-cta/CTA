/******************************************************************************
 *         castor/legacymsg/TapeserverProxyDummy.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/TapeserverProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A dummy tapeserverd-proxy.
 */
class TapeserverProxyDummy: public TapeserverProxy {
public:

  /**
   * Constructor.
   */
  TapeserverProxyDummy() throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~TapeserverProxyDummy() throw();

  /**
   * Sets the VID of the tape mounted in the specified tape drive.
   *
   * @param vid The Volume ID of the tape in the tape drive
   * @param unitName The unit name of the tape drive.
   */
  void setVidInDriveCatalogue(const std::string &vid, const std::string &unitName) throw(castor::exception::Exception);

}; // class TapeserverProxyDummy

} // namespace legacymsg
} // namespace castor

