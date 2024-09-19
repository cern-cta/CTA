/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <optional>
#include <string>

#include "catalogue/TapePoolSearchCriteria.hpp"
#include "common/exception/UserError.hpp"

namespace cta {

namespace common::dataStructures {
struct SecurityIdentity;
}

namespace catalogue {

class TapePool;

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringTapePoolName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyTapePool);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTapePool);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedTapePoolUsedInAnArchiveRoute);

class TapePoolCatalogue {
public:
  virtual ~TapePoolCatalogue() = default;

  virtual void createTapePool(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& name,
                              const std::string& vo,
                              const uint64_t nbPartialTapes,
                              const bool encryptionValue,
                              const std::list<std::string>& supply_list,
                              const std::string& comment) = 0;

  virtual void deleteTapePool(const std::string &name) = 0;

  virtual std::list<TapePool> getTapePools(
    const TapePoolSearchCriteria &searchCriteria = TapePoolSearchCriteria()) const = 0;

  virtual std::optional<TapePool> getTapePool(const std::string &tapePoolName) const = 0;

  virtual void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) = 0;

  virtual void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t nbPartialTapes) = 0;

  virtual void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) = 0;

  virtual void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool encryptionValue) = 0;

  virtual void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                                    const std::string& name,
                                    const std::list<std::string>& supply_list) = 0;

  virtual void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName,
    const std::string &newName) = 0;

  /**
   * Returns true if the specified tape pool exists.
   *
   * @param tapePoolName The name of the tape pool.
   * @return True if the tape pool exists.
   */
  virtual bool tapePoolExists(const std::string &tapePoolName) const = 0;
  virtual void deleteAllTapePoolSupplyEntries() = 0;
};

}} // namespace cta::catalogue
