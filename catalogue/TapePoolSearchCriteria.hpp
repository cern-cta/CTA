/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <string>
#include <vector>

#include <common/optional.hpp>

namespace cta {
namespace catalogue {

/**
 * The collection of criteria used to select a set of tapepools.
 *
 * A tapepool is selected if it meets all of the specified criteria.
 *
 * A criterion is only considered specified if it has been set.
 *
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct TapePoolSearchCriteria {

   /**
   * The name of the tapepool.
   */
    optional<std::string> name;

    /**
    * The virtual organization of the tapepool.
    */
    optional<std::string> vo;

    /**
    * Set to true if searching for encrypted tape pools.
    */
    optional<bool> encrypted;

}; // struct TapePoolSearchCriteria

} // namespace catalogue
} // namespace cta