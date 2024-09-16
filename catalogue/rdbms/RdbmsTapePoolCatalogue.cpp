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

#include <list>
#include <optional>
#include <string>
#include <regex>

#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsTapePoolCatalogue.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsTapePoolCatalogue::RdbmsTapePoolCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsTapePoolCatalogue::createTapePool(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue,
  const std::optional<std::string> &supply, const std::string &comment) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape pool because the tape pool name is an empty string");
  }

  if(vo.empty()) {
    throw UserSpecifiedAnEmptyStringVo("Cannot create tape pool because the VO is an empty string");
  }

  if (comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot create tape pool because the comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

  auto conn = m_connPool->getConn();

  if(RdbmsCatalogueUtils::tapePoolExists(conn, name)) {
    throw exception::UserError(std::string("Cannot create tape pool ") + name +
      " because a tape pool with the same name already exists");
  }
  if(!RdbmsCatalogueUtils::virtualOrganizationExists(conn,vo)){
    throw exception::UserError(std::string("Cannot create tape pool ") + name + \
      " because vo : "+vo+" does not exist.");
  }
  const uint64_t tapePoolId = getNextTapePoolId(conn);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    INSERT INTO TAPE_POOL(
      TAPE_POOL_ID,
      TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION_ID,
      NB_PARTIAL_TAPES,
      IS_ENCRYPTED,
      SUPPLY,

      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME)
    SELECT 
      :TAPE_POOL_ID,
      :TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION_ID,
      :NB_PARTIAL_TAPES,
      :IS_ENCRYPTED,
      :SUPPLY,

      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME 
    FROM 
      VIRTUAL_ORGANIZATION 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VO
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindUint64(":TAPE_POOL_ID", tapePoolId);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.bindString(":VO", vo);
  stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
  stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
  stmt.bindString(":SUPPLY", supply);

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();

  std::vector<std::string> verified_matches = verifyTapePoolSupply(conn, supply.value_or(""));
  populateSupplyTable(conn, name, verified_matches);
}

/**
 * Returns a vector of the extracted tape pool names from a comma-separated list
 */
std::vector<std::string> RdbmsTapePoolCatalogue::verifyTapePoolSupply(rdbms::Conn &conn, const std::string &supply) {
  const std::regex CTA_SUPPLY_OPTION_REGEX("^\\s*([^,]+\\s*,\\s*)*[^,]+\\s*$");
  std::vector<std::string> verified_matches;
  bool valid = std::regex_match(supply, CTA_SUPPLY_OPTION_REGEX);
  if (!valid && !supply.empty())
  {
    throw UserSpecifiedAnInvalidSupplyString("Cannot set tape pool supply because user specified an invalid supply string: \"" + supply +"\"");
  }
  // for every submatch, verify the tape pool exists
  std::regex pattern_between_commas("\\s*([^,]+)\\s*,?");
  auto it_begin = std::sregex_iterator(supply.begin(), supply.end(), pattern_between_commas);
  auto it_end = std::sregex_iterator();

  for (std::sregex_iterator it = it_begin; it != it_end; ++it){
    std::smatch match = *it;
    bool exists = RdbmsCatalogueUtils::tapePoolExists(conn, match[1]);
    if (!exists)
    {
      throw UserSpecifiedAnInvalidSupplyString("Cannot set tape pool supply because tape pool \"" + std::string(match[1]) + "\" does not exist");
    }
    verified_matches.push_back(std::string(match[1]));
  }
  return verified_matches;
}

std::pair<std::map<std::string, std::set<std::string>>, std::map<std::string, std::set<std::string>>> RdbmsTapePoolCatalogue::getAllTapePoolSupplySourcesAndDestinations(rdbms::Conn &conn) const {
  std::string sql = R"SQL(
    SELECT
      TP_SRC.TAPE_POOL_NAME AS SUPPLY_SRC_TAPE_POOL_NAME,
      TP_DST.TAPE_POOL_NAME AS SUPPLY_DST_TAPE_POOL_NAME
    FROM TAPE_POOL TP_SRC
    INNER JOIN TAPE_POOL_SUPPLY SP ON TP_SRC.TAPE_POOL_ID = SP.SUPPLY_SOURCE_TAPE_POOL_ID
    INNER JOIN TAPE_POOL TP_DST ON SP.SUPPLY_DESTINATION_TAPE_POOL_ID = TP_DST.TAPE_POOL_ID
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::map<std::string, std::set<std::string>> sources_per_tape_pool;
  std::map<std::string, std::set<std::string>> destinations_per_tape_pool;

  while (rset.next()) {
    auto tape_pool_src = rset.columnString("SUPPLY_SRC_TAPE_POOL_NAME");
    auto tape_pool_dst = rset.columnString("SUPPLY_DST_TAPE_POOL_NAME");
    sources_per_tape_pool[tape_pool_dst].insert(tape_pool_src);
    destinations_per_tape_pool[tape_pool_src].insert(tape_pool_dst);
  }

  return std::pair(std::move(sources_per_tape_pool), std::move(destinations_per_tape_pool));
}

std::pair<std::set<std::string>, std::set<std::string>> RdbmsTapePoolCatalogue::getTapePoolSupplySourcesAndDestinations(rdbms::Conn &conn, const std::string &tapePoolName) const {
  std::string sql = R"SQL(
    SELECT
      TP_SRC.TAPE_POOL_NAME AS SUPPLY_SRC_TAPE_POOL_NAME,
      TP_DST.TAPE_POOL_NAME AS SUPPLY_DST_TAPE_POOL_NAME
    FROM TAPE_POOL TP_SRC
    INNER JOIN TAPE_POOL_SUPPLY SP ON TP_SRC.TAPE_POOL_ID = SP.SUPPLY_SOURCE_TAPE_POOL_ID
    INNER JOIN TAPE_POOL TP_DST ON SP.SUPPLY_DESTINATION_TAPE_POOL_ID = TP_DST.TAPE_POOL_ID
    WHERE SUPPLY_SRC_TAPE_POOL_NAME = :SUPPLY_SRC_TAPE_POOL_NAME OR SUPPLY_DST_TAPE_POOL_NAME = :SUPPLY_DST_TAPE_POOL_NAME
  )SQL";

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":SUPPLY_SRC_TAPE_POOL_NAME", tapePoolName);
  stmt.bindString(":SUPPLY_DST_TAPE_POOL_NAME", tapePoolName);
  auto rset = stmt.executeQuery();

  std::set<std::string> sources, destinations;
  while (rset.next()) {
    auto tape_pool_src = rset.columnString("SUPPLY_SRC_TAPE_POOL_NAME");
    auto tape_pool_dst = rset.columnString("SUPPLY_DST_TAPE_POOL_NAME");
    if (tape_pool_src == tapePoolName) {
      destinations.insert(tape_pool_dst);
    }
    if (tape_pool_dst == tapePoolName) {
      sources.insert(tape_pool_src);
    }
  }

  return std::pair(std::move(sources), std::move(destinations));
}

void RdbmsTapePoolCatalogue::deleteTapePool(const std::string &name) {
  auto conn = m_connPool->getConn();

  if(tapePoolUsedInAnArchiveRoute(conn, name)) {
    UserSpecifiedTapePoolUsedInAnArchiveRoute ex;
    ex.getMessage() << "Cannot delete tape-pool " << name << " because it is used in an archive route";
    throw ex;
  }

  const uint64_t nbTapesInPool = getNbTapesInPool(conn, name);

  if(0 == nbTapesInPool) {
    const char* const sql = R"SQL(
      DELETE FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME
    )SQL";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete tape-pool ") + name + " because it does not exist");
    }

    m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();

  } else {
    throw UserSpecifiedAnEmptyTapePool(std::string("Cannot delete tape-pool ") + name + " because it is not empty");
  }
}

std::list<TapePool> RdbmsTapePoolCatalogue::getTapePools(const TapePoolSearchCriteria &searchCriteria) const {
  auto conn = m_connPool->getConn();
  return getTapePools(conn, searchCriteria);
}

std::list<TapePool> RdbmsTapePoolCatalogue::getTapePools(rdbms::Conn &conn,
  const TapePoolSearchCriteria &searchCriteria) const {
  if (RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.name))
    throw exception::UserError("Pool name cannot be an empty string");
  if (RdbmsCatalogueUtils::isSetAndEmpty(searchCriteria.vo))
    throw exception::UserError("Virtual organisation cannot be an empty string");

  if (searchCriteria.name && !RdbmsCatalogueUtils::tapePoolExists(conn, searchCriteria.name.value())) {
    UserSpecifiedANonExistentTapePool ex;
    ex.getMessage() << "Cannot list tape pools because tape pool " + searchCriteria.name.value() + " does not exist";
    throw ex;
  }

  if (searchCriteria.vo
    && !RdbmsCatalogueUtils::virtualOrganizationExists(conn, searchCriteria.vo.value())) {
    UserSpecifiedANonExistentVirtualOrganization ex;
    ex.getMessage() << "Cannot list tape pools because virtual organization " + searchCriteria.vo.value() + " does not exist";
    throw ex;
  }

  std::list<TapePool> pools;
  std::string sql = R"SQL(
    SELECT 
      TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,
      TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,
      TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,
      TAPE_POOL.SUPPLY AS SUPPLY,

      COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.DATA_IN_BYTES = 0 THEN 1 ELSE 0 END), 0) AS NB_EMPTY_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_DISABLED THEN 1 ELSE 0 END), 0) AS NB_DISABLED_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.IS_FULL <> '0' THEN 1 ELSE 0 END), 0) AS NB_FULL_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_ACTIVE AND TAPE.IS_FULL = '0' THEN 1 ELSE 0 END), 0) AS NB_WRITABLE_TAPES,
      COALESCE(SUM(MEDIA_TYPE.CAPACITY_IN_BYTES), 0) AS CAPACITY_IN_BYTES,
      COALESCE(SUM(TAPE.DATA_IN_BYTES), 0) AS DATA_IN_BYTES,
      COALESCE(SUM(TAPE.LAST_FSEQ), 0) AS NB_PHYSICAL_FILES,

      TAPE_POOL.USER_COMMENT AS USER_COMMENT,

      TAPE_POOL.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      TAPE_POOL.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      TAPE_POOL.CREATION_LOG_TIME AS CREATION_LOG_TIME,

      TAPE_POOL.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      TAPE_POOL.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      TAPE_POOL.LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      TAPE_POOL 
    INNER JOIN VIRTUAL_ORGANIZATION ON 
      TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID 
    LEFT OUTER JOIN TAPE ON 
      TAPE_POOL.TAPE_POOL_ID = TAPE.TAPE_POOL_ID 
    LEFT OUTER JOIN MEDIA_TYPE ON 
      TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID
  )SQL";

  if (searchCriteria.name || searchCriteria.vo || searchCriteria.encrypted) {
    sql += R"SQL(
      WHERE
    )SQL";
  }
  bool addedAWhereConstraint = false;
  if (searchCriteria.name) {
    sql += R"SQL(
      TAPE_POOL.TAPE_POOL_NAME = :NAME
    )SQL";
    addedAWhereConstraint = true;
  }

  if (searchCriteria.vo) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME = :VO
    )SQL";
    addedAWhereConstraint = true;
  }

  if (searchCriteria.encrypted) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE_POOL.IS_ENCRYPTED = :ENCRYPTED
    )SQL";
  }

  sql += R"SQL(
    GROUP BY 
      TAPE_POOL.TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME,
      TAPE_POOL.NB_PARTIAL_TAPES,
      TAPE_POOL.IS_ENCRYPTED,
      TAPE_POOL.SUPPLY,
      TAPE_POOL.USER_COMMENT,
      TAPE_POOL.CREATION_LOG_USER_NAME,
      TAPE_POOL.CREATION_LOG_HOST_NAME,
      TAPE_POOL.CREATION_LOG_TIME,
      TAPE_POOL.LAST_UPDATE_USER_NAME,
      TAPE_POOL.LAST_UPDATE_HOST_NAME,
      TAPE_POOL.LAST_UPDATE_TIME 
    ORDER BY 
      TAPE_POOL_NAME
  )SQL";

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STATE_DISABLED",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::DISABLED));
  stmt.bindString(":STATE_ACTIVE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));

  if (searchCriteria.name) {
    stmt.bindString(":NAME", searchCriteria.name.value());
  }

  if (searchCriteria.vo) {
    stmt.bindString(":VO", searchCriteria.vo.value());
  }

  if(searchCriteria.encrypted) {
    stmt.bindBool(":ENCRYPTED", searchCriteria.encrypted.value());
  }

  auto rset = stmt.executeQuery();
  while (rset.next()) {
    TapePool pool;
    pool.name = rset.columnString("TAPE_POOL_NAME");
    pool.vo.name = rset.columnString("VO");
    pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
    pool.encryption = rset.columnBool("IS_ENCRYPTED");
    pool.supply = rset.columnOptionalString("SUPPLY");
    pool.nbTapes = rset.columnUint64("NB_TAPES");
    pool.nbEmptyTapes = rset.columnUint64("NB_EMPTY_TAPES");
    pool.nbDisabledTapes = rset.columnUint64("NB_DISABLED_TAPES");
    pool.nbFullTapes = rset.columnUint64("NB_FULL_TAPES");
    pool.nbWritableTapes = rset.columnUint64("NB_WRITABLE_TAPES");
    pool.capacityBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    pool.dataBytes = rset.columnUint64("DATA_IN_BYTES");
    pool.nbPhysicalFiles = rset.columnUint64("NB_PHYSICAL_FILES");
    pool.comment = rset.columnString("USER_COMMENT");
    pool.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    pool.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    pool.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    pool.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    pool.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    pool.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    pools.push_back(pool);
  }

  // Get all supply source and destination tapepools, in a single go
  auto [supply_src_per_tape_pool, supply_dst_per_tape_pool] = getAllTapePoolSupplySourcesAndDestinations(conn);

  for (auto &pool : pools) {
    auto src_set_node_handle = supply_src_per_tape_pool.extract(pool.name);
    auto dst_set_node_handle = supply_dst_per_tape_pool.extract(pool.name);
    if (src_set_node_handle) {
      pool.supply_source_set = std::move(src_set_node_handle.mapped());
    }
    if (dst_set_node_handle) {
      pool.supply_destination_set = std::move(dst_set_node_handle.mapped());
    }
  }

  return pools;
}

std::optional<TapePool> RdbmsTapePoolCatalogue::getTapePool(const std::string &tapePoolName) const {
  const char* const sql = R"SQL(
    SELECT 
      TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,
      TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,
      TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,
      TAPE_POOL.SUPPLY AS SUPPLY,

      COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.DATA_IN_BYTES = 0 THEN 1 ELSE 0 END), 0) AS NB_EMPTY_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_DISABLED THEN 1 ELSE 0 END), 0) AS NB_DISABLED_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.IS_FULL <> '0' THEN 1 ELSE 0 END), 0) AS NB_FULL_TAPES,
      COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_ACTIVE AND TAPE.IS_FULL = '0' THEN 1 ELSE 0 END), 0) AS NB_WRITABLE_TAPES,
      COALESCE(SUM(MEDIA_TYPE.CAPACITY_IN_BYTES), 0) AS CAPACITY_IN_BYTES,
      COALESCE(SUM(TAPE.DATA_IN_BYTES), 0) AS DATA_IN_BYTES,
      COALESCE(SUM(TAPE.LAST_FSEQ), 0) AS NB_PHYSICAL_FILES,

      TAPE_POOL.USER_COMMENT AS USER_COMMENT,

      TAPE_POOL.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      TAPE_POOL.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      TAPE_POOL.CREATION_LOG_TIME AS CREATION_LOG_TIME,

      TAPE_POOL.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      TAPE_POOL.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      TAPE_POOL.LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      TAPE_POOL 
    INNER JOIN VIRTUAL_ORGANIZATION ON 
      TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID 
    LEFT OUTER JOIN TAPE ON 
      TAPE_POOL.TAPE_POOL_ID = TAPE.TAPE_POOL_ID 
    LEFT OUTER JOIN MEDIA_TYPE ON 
      TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID 
    GROUP BY 
      TAPE_POOL.TAPE_POOL_NAME,
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME,
      TAPE_POOL.NB_PARTIAL_TAPES,
      TAPE_POOL.IS_ENCRYPTED,
      TAPE_POOL.SUPPLY,
      TAPE_POOL.USER_COMMENT,
      TAPE_POOL.CREATION_LOG_USER_NAME,
      TAPE_POOL.CREATION_LOG_HOST_NAME,
      TAPE_POOL.CREATION_LOG_TIME,
      TAPE_POOL.LAST_UPDATE_USER_NAME,
      TAPE_POOL.LAST_UPDATE_HOST_NAME,
      TAPE_POOL.LAST_UPDATE_TIME 
    HAVING 
      TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME 
    ORDER BY 
      TAPE_POOL_NAME
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
  stmt.bindString(":STATE_DISABLED",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::DISABLED));
  stmt.bindString(":STATE_ACTIVE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));

  auto rset = stmt.executeQuery();

  if (!rset.next()) {
    return std::nullopt;
  }

  TapePool pool;
  pool.name = rset.columnString("TAPE_POOL_NAME");
  pool.vo.name = rset.columnString("VO");
  pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
  pool.encryption = rset.columnBool("IS_ENCRYPTED");
  pool.supply = rset.columnOptionalString("SUPPLY");
  pool.nbTapes = rset.columnUint64("NB_TAPES");
  pool.nbEmptyTapes = rset.columnUint64("NB_EMPTY_TAPES");
  pool.nbDisabledTapes = rset.columnUint64("NB_DISABLED_TAPES");
  pool.nbFullTapes = rset.columnUint64("NB_FULL_TAPES");
  pool.nbWritableTapes = rset.columnUint64("NB_WRITABLE_TAPES");
  pool.capacityBytes = rset.columnUint64("CAPACITY_IN_BYTES");
  pool.dataBytes = rset.columnUint64("DATA_IN_BYTES");
  pool.nbPhysicalFiles = rset.columnUint64("NB_PHYSICAL_FILES");
  pool.comment = rset.columnString("USER_COMMENT");
  pool.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
  pool.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
  pool.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
  pool.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
  pool.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
  pool.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

  {
    auto [supply_src_set, supply_dst_set] = getTapePoolSupplySourcesAndDestinations(conn, pool.name);
    pool.supply_source_set = std::move(supply_src_set);
    pool.supply_destination_set = std::move(supply_dst_set);
  }

  return pool;
}

void RdbmsTapePoolCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
      " string");
  }

  if(vo.empty()) {
    throw UserSpecifiedAnEmptyStringVo("Cannot modify tape pool because the new VO is an empty string");
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      VIRTUAL_ORGANIZATION_ID = (SELECT VIRTUAL_ORGANIZATION_ID FROM VIRTUAL_ORGANIZATION WHERE VIRTUAL_ORGANIZATION_NAME=:VO),
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  if(!RdbmsCatalogueUtils::virtualOrganizationExists(conn,vo)){
    throw exception::UserError(std::string("Cannot modify tape pool ") + name +" because the vo " + vo + " does not exist");
  }

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VO", vo);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
  }
  //The VO of this tapepool has changed, invalidate the tapepool-VO cache
  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsTapePoolCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
      " string");
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      NB_PARTIAL_TAPES = :NB_PARTIAL_TAPES,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
  }
}

void RdbmsTapePoolCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
      " string");
  }

  if(comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot modify tape pool because the new comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      USER_COMMENT = :USER_COMMENT,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
  }
}

void RdbmsTapePoolCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool encryptionValue) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      IS_ENCRYPTED = :IS_ENCRYPTED,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
  }
}

void RdbmsTapePoolCatalogue::populateSupplyTable(rdbms::Conn &conn, std::string tapePoolName, std::vector<std::string> verified_matches) {
  /* wipe the previous supply sources for this tapepool, as we are resetting them now */
  std::string sql_drop_old = 
  "DELETE FROM TAPE_POOL_SUPPLY WHERE SUPPLY_DESTINATION_TAPE_POOL_ID = "
  "(SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME)";
  auto stmt_delete = conn.createStmt(sql_drop_old);
  stmt_delete.bindString(":TAPE_POOL_NAME", tapePoolName);
  stmt_delete.executeNonQuery();

  for (auto match : verified_matches) {
    std::string sql = 
    "INSERT INTO TAPE_POOL_SUPPLY (SUPPLY_SOURCE_TAPE_POOL_ID, SUPPLY_DESTINATION_TAPE_POOL_ID) "
    " SELECT SRC.TAPE_POOL_ID AS SUPPLY_SOURCE_TAPE_POOL_ID, DEST.TAPE_POOL_ID AS SUPPLY_DESTINATION_TAPE_POOL_ID "
    " FROM "
    " (SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :SUPPLY_SOURCE_NAME) SRC, "
    " (SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :SUPPLY_DESTINATION_NAME) DEST ";

    auto stmt_insert = conn.createStmt(sql);
    stmt_insert.bindString(":SUPPLY_SOURCE_NAME", match);
    stmt_insert.bindString(":SUPPLY_DESTINATION_NAME", tapePoolName);
    stmt_insert.executeNonQuery();
  }
}

void RdbmsTapePoolCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &supply) {
  if(name.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
      " string");
  }
  std::vector<std::string> verified_matches;
  std::optional<std::string> optionalSupply;
  auto conn = m_connPool->getConn();
  if(!supply.empty()) {
    verified_matches = verifyTapePoolSupply(conn, supply);
    optionalSupply = supply;
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      SUPPLY = :SUPPLY,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":SUPPLY", optionalSupply);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":TAPE_POOL_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
  }

  populateSupplyTable(conn, name, verified_matches);
}

void RdbmsTapePoolCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  if(currentName.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
      " string");
  }

  if(newName.empty()) {
    throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the new name is an empty string");
  }

  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE_POOL SET 
      TAPE_POOL_NAME = :NEW_TAPE_POOL_NAME,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      TAPE_POOL_NAME = :CURRENT_TAPE_POOL_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":NEW_TAPE_POOL_NAME", newName);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":CURRENT_TAPE_POOL_NAME", currentName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify tape pool ") + currentName + " because it does not exist");
  }

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

bool RdbmsTapePoolCatalogue::tapePoolUsedInAnArchiveRoute(rdbms::Conn &conn, const std::string &tapePoolName) const {
  const char* const sql = R"SQL(
    SELECT 
      TAPE_POOL_NAME AS TAPE_POOL_NAME 
    FROM 
      TAPE_POOL 
    INNER JOIN ARCHIVE_ROUTE ON 
      TAPE_POOL.TAPE_POOL_ID = ARCHIVE_ROUTE.TAPE_POOL_ID 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

uint64_t RdbmsTapePoolCatalogue::getNbTapesInPool(rdbms::Conn &conn, const std::string &name) const {
  const char* const sql = R"SQL(
    SELECT 
      COUNT(*) AS NB_TAPES 
    FROM 
      TAPE 
    INNER JOIN TAPE_POOL ON 
      TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID 
    WHERE 
      TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME", name);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception("Result set of SELECT COUNT(*) is empty");
  }
  return rset.columnUint64("NB_TAPES");
}

std::optional<uint64_t> RdbmsTapePoolCatalogue::getTapePoolId(rdbms::Conn &conn, const std::string &name) const {
  const char* const sql = R"SQL(
    SELECT 
      TAPE_POOL_ID AS TAPE_POOL_ID 
    FROM 
      TAPE_POOL 
    WHERE 
      TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME", name);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    return std::nullopt;
  }
  return rset.columnUint64("TAPE_POOL_ID");
}

bool RdbmsTapePoolCatalogue::tapePoolExists(const std::string &tapePoolName) const {
  auto conn = m_connPool->getConn();
  return RdbmsCatalogueUtils::tapePoolExists(conn, tapePoolName);
}

void RdbmsTapePoolCatalogue::deleteAllTapePoolSupplyEntries() {
  auto conn = m_connPool->getConn();
  deleteAllTapePoolSupplyEntries(conn);
}

void RdbmsTapePoolCatalogue::deleteAllTapePoolSupplyEntries(rdbms::Conn &conn) {
  std::string sql = "DELETE FROM TAPE_POOL_SUPPLY";
  auto stmt = conn.createStmt(sql);
  stmt.executeNonQuery();
}

} // namespace cta::catalogue
