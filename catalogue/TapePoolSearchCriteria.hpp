/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

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
  std::optional<std::string> name;

  /**
    * The virtual organization of the tapepool.
    */
  std::optional<std::string> vo;

  /**
    * Set to true if searching for encrypted tape pools.
    */
  std::optional<bool> encrypted;

};  // struct TapePoolSearchCriteria

}  // namespace catalogue
}  // namespace cta
