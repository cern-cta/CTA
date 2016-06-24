/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>

namespace cta {
namespace catalogue {

/**
 * The collection of criteria used to select a set of tapes.
 *
 * A tape is selected if it meets all of the specified criteria.
 *
 * A criterion is only considered specified if it has been set.
 *
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct TapeSearchCriteria {

  /**
   * The volume identifier of a tape.
   */
  std::string vid;

  /**
   * The name of a logical library.
   */
  std::string logicalLibrary;

  /**
   * The name of a tape pool.
   */
  std::string tapePool;

  /**
   * The capacity of a tape in bytes
   */
  std::string capacityInBytes;

  /**
   * Set to true if searching for disabled tapes.
   */
  std::string disabled;

  /**
   * Set to true if searching for full tapes.
   */
  std::string full;

  /**
   * Set to true if searching for tapes with logical block protection enabled.
   */
  std::string lbp;

}; // struct TapeSearchCriteria

} // namespace catalogue
} // namespace cta
