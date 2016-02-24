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

#include "castor/log/Logger.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * The contents of the castor.conf file to be used by a LabelSession.
 */
struct LabelSessionConfig {

  /**
   * The boolean variable describing to use on not to use Logical 
   * Block Protection.
   */
  bool useLbp;

  
  /**
   * Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string and all boolean member-variables 
   * to false.
   */
  LabelSessionConfig() throw();

  /**
   * Returns a configuration structure based on the contents of
   * /etc/castor/castor.conf and compile-time constants.
   *
   * @param log pointer to NULL or an optional logger object.
   * @return The configuration structure.
   */
  static LabelSessionConfig createFromCastorConf(
    log::Logger *const log = NULL);

}; // LabelSessionConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
