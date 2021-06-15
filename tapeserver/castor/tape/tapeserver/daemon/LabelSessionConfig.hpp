/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/log/Logger.hpp"

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

}; // LabelSessionConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
