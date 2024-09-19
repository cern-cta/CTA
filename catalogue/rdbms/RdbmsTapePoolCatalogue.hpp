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
#include <set>
#include <memory>
#include <optional>
#include <string>

#include "catalogue/interfaces/TapePoolCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsTapePoolCatalogue : public TapePoolCatalogue {
public:
  ~RdbmsTapePoolCatalogue() override = default;

  void createTapePool(const common::dataStructures::SecurityIdentity& admin,
                      const std::string& name,
                      const std::string& vo,
                      const uint64_t nbPartialTapes,
                      const bool encryptionValue,
                      const std::list<std::string>& supply_list,
                      const std::string& comment) override;

  void deleteTapePool(const std::string &name) override;

  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const override;

  std::optional<TapePool> getTapePool(const std::string &tapePoolName) const override;

  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) override;

  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const uint64_t nbPartialTapes) override;

  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) override;

  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool encryptionValue) override;

  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const std::list<std::string>& supply_list) override;

  void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName,
    const std::string &newName) override;

  bool tapePoolExists(const std::string &tapePoolName) const override;

  void deleteAllTapePoolSupplyEntries() override;

protected:
  RdbmsTapePoolCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue);

private:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  /**
   * Returns a unique tape pool ID that can be used by a new tape pool within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique tape pool ID that can be used by a new tape pool within
   * the catalogue.
   */
  virtual uint64_t getNextTapePoolId(rdbms::Conn &conn) const = 0;

  std::list<TapePool> getTapePools(rdbms::Conn &conn, const TapePoolSearchCriteria &searchCriteria) const;

  /**
   * Returns true if the specified tape pool is used in an archive route.
   *
   * @param conn The database connection.
   * @param tapePoolName The name of the tape pool.
   * @return True if the tape pool is used in an archive route.
   */
  bool tapePoolUsedInAnArchiveRoute(rdbms::Conn &conn, const std::string &tapePoolName) const;

  /**
   * Returns the number of tapes in the specified tape pool.
   *
   * If the tape pool does not exist then this method returns 0.
   *
   * @param conn The database connection.
   * @param name The name of the tape pool.
   * @return The number of tapes in the specified tape pool.
   */
  uint64_t getNbTapesInPool(rdbms::Conn &conn, const std::string &name) const;

  friend class RdbmsTapeCatalogue;
  std::map<std::string, uint64_t> getTapePoolIdMap(rdbms::Conn &conn, const std::vector<std::string> &names) const;

  std::pair<std::map<std::string, std::set<std::string>>, std::map<std::string, std::set<std::string>>>
  getAllTapePoolSupplySourcesAndDestinations(rdbms::Conn& conn) const;
  std::pair<std::set<std::string>, std::set<std::string>>
  getTapePoolSupplySourcesAndDestinations(rdbms::Conn& conn, const std::string& tapePoolName) const;

  void populateSupplyTable(rdbms::Conn& conn, std::string tapePoolName, std::list<std::string> supply_list);
  void deleteAllTapePoolSupplyEntries(rdbms::Conn& conn);
};

}} // namespace cta::catalogue
