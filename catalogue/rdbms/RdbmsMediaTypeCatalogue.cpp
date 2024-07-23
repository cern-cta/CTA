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

#include <string>

#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsMediaTypeCatalogue::RdbmsMediaTypeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsMediaTypeCatalogue::createMediaType(const common::dataStructures::SecurityIdentity &admin,
  const MediaType &mediaType) {
  if (mediaType.name.empty()) {
    throw UserSpecifiedAnEmptyStringMediaTypeName("Cannot create media type because the media type name is an"
      " empty string");
  }

  if (mediaType.cartridge.empty()) {
    throw UserSpecifiedAnEmptyStringCartridge(std::string("Cannot create media type ") + mediaType.name +
      " because the cartridge is an empty string");
  }

  if (mediaType.comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment(std::string("Cannot create media type ") + mediaType.name +
      " because the comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(mediaType.comment, &m_log);
  if (mediaType.capacityInBytes == 0){
    throw UserSpecifiedAZeroCapacity(std::string("Cannot create media type ") + mediaType.name
      + " because the capacity is zero");
  }
  auto conn = m_connPool->getConn();
  if (RdbmsCatalogueUtils::mediaTypeExists(conn, mediaType.name)) {
    throw exception::UserError(std::string("Cannot create media type ") + mediaType.name +
      " because it already exists");
  }
  const uint64_t mediaTypeId = getNextMediaTypeId(conn);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    INSERT INTO MEDIA_TYPE(
      MEDIA_TYPE_ID,
      MEDIA_TYPE_NAME,
      CARTRIDGE,
      CAPACITY_IN_BYTES,
      PRIMARY_DENSITY_CODE,
      SECONDARY_DENSITY_CODE,
      NB_WRAPS,
      MIN_LPOS,
      MAX_LPOS,

      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME)
    VALUES(
      :MEDIA_TYPE_ID,
      :MEDIA_TYPE_NAME,
      :CARTRIDGE,
      :CAPACITY_IN_BYTES,
      :PRIMARY_DENSITY_CODE,
      :SECONDARY_DENSITY_CODE,
      :NB_WRAPS,
      :MIN_LPOS,
      :MAX_LPOS,

      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME)
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindUint64(":MEDIA_TYPE_ID", mediaTypeId);
  stmt.bindString(":MEDIA_TYPE_NAME", mediaType.name);
  stmt.bindString(":CARTRIDGE", mediaType.cartridge);
  stmt.bindUint64(":CAPACITY_IN_BYTES", mediaType.capacityInBytes);
  stmt.bindUint8(":PRIMARY_DENSITY_CODE", mediaType.primaryDensityCode);
  stmt.bindUint8(":SECONDARY_DENSITY_CODE", mediaType.secondaryDensityCode);
  stmt.bindUint32(":NB_WRAPS", mediaType.nbWraps);
  stmt.bindUint64(":MIN_LPOS", mediaType.minLPos);
  stmt.bindUint64(":MAX_LPOS", mediaType.maxLPos);

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();
}

void RdbmsMediaTypeCatalogue::deleteMediaType(const std::string &name) {
  auto conn = m_connPool->getConn();

  if(mediaTypeIsUsedByTapes(conn, name)) {
    throw UserSpecifiedMediaTypeUsedByTapes(std::string("The ") + name +
      " media type is being used by one or more tapes");
  }

  const char* const sql = R"SQL(
    DELETE FROM 
      MEDIA_TYPE 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":MEDIA_TYPE_NAME", name);

  stmt.executeNonQuery();
  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot delete media type ") + name + " because it does not exist");
  }
}

std::list<MediaTypeWithLogs> RdbmsMediaTypeCatalogue::getMediaTypes() const {
  std::list<MediaTypeWithLogs> mediaTypes;
  const char* const sql = R"SQL(
    SELECT 
      MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME,
      CARTRIDGE AS CARTRIDGE,
      CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,
      PRIMARY_DENSITY_CODE AS PRIMARY_DENSITY_CODE,
      SECONDARY_DENSITY_CODE AS SECONDARY_DENSITY_CODE,
      NB_WRAPS AS NB_WRAPS,
      MIN_LPOS AS MIN_LPOS,
      MAX_LPOS AS MAX_LPOS,

      USER_COMMENT AS USER_COMMENT,

      CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME AS CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      MEDIA_TYPE 
    ORDER BY 
      MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    MediaTypeWithLogs mediaType;

    mediaType.name = rset.columnString("MEDIA_TYPE_NAME");
    mediaType.cartridge = rset.columnString("CARTRIDGE");
    mediaType.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    mediaType.primaryDensityCode = rset.columnOptionalUint8("PRIMARY_DENSITY_CODE");
    mediaType.secondaryDensityCode = rset.columnOptionalUint8("SECONDARY_DENSITY_CODE");
    mediaType.nbWraps = rset.columnOptionalUint32("NB_WRAPS");
    mediaType.minLPos = rset.columnOptionalUint64("MIN_LPOS");
    mediaType.maxLPos = rset.columnOptionalUint64("MAX_LPOS");
    mediaType.comment = rset.columnString("USER_COMMENT");
    mediaType.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    mediaType.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    mediaType.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    mediaType.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    mediaType.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    mediaType.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    mediaTypes.push_back(mediaType);
  }

  return mediaTypes;
}

MediaType RdbmsMediaTypeCatalogue::getMediaTypeByVid(const std::string & vid) const {
  std::list<MediaTypeWithLogs> mediaTypes;
  const char* const sql = R"SQL(
    SELECT 
      MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME,
      CARTRIDGE AS CARTRIDGE,
      CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,
      PRIMARY_DENSITY_CODE AS PRIMARY_DENSITY_CODE,
      SECONDARY_DENSITY_CODE AS SECONDARY_DENSITY_CODE,
      NB_WRAPS AS NB_WRAPS,
      MIN_LPOS AS MIN_LPOS,
      MAX_LPOS AS MAX_LPOS,

      MEDIA_TYPE.USER_COMMENT AS USER_COMMENT 
    FROM 
      MEDIA_TYPE 
    INNER JOIN TAPE 
      ON MEDIA_TYPE.MEDIA_TYPE_ID = TAPE.MEDIA_TYPE_ID 
    WHERE 
      TAPE.VID = :VID
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID",vid);
  auto rset = stmt.executeQuery();
  if(rset.next()){
    MediaType mediaType;

    mediaType.name = rset.columnString("MEDIA_TYPE_NAME");
    mediaType.cartridge = rset.columnString("CARTRIDGE");
    mediaType.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    mediaType.primaryDensityCode = rset.columnOptionalUint8("PRIMARY_DENSITY_CODE");
    mediaType.secondaryDensityCode = rset.columnOptionalUint8("SECONDARY_DENSITY_CODE");
    mediaType.nbWraps = rset.columnOptionalUint32("NB_WRAPS");
    mediaType.minLPos = rset.columnOptionalUint64("MIN_LPOS");
    mediaType.maxLPos = rset.columnOptionalUint64("MAX_LPOS");
    mediaType.comment = rset.columnString("USER_COMMENT");

    return mediaType;
  } else {
    throw exception::Exception("The tape vid "+vid+" does not exist.");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      MEDIA_TYPE_NAME = :NEW_MEDIA_TYPE_NAME,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :CURRENT_MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  if(newName != currentName && RdbmsCatalogueUtils::mediaTypeExists(conn, newName)){
    throw exception::UserError(std::string("Cannot modify the media type name ") + currentName +". The new name : "
    + newName + " already exists in the database.");
  }
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":NEW_MEDIA_TYPE_NAME", newName);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":CURRENT_MEDIA_TYPE_NAME", currentName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + currentName + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &cartridge) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      CARTRIDGE = :CARTRIDGE,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":CARTRIDGE", cartridge);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t capacityInBytes) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint8_t primaryDensityCode) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      PRIMARY_DENSITY_CODE = :PRIMARY_DENSITY_CODE,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint8(":PRIMARY_DENSITY_CODE", primaryDensityCode);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint8_t secondaryDensityCode) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      SECONDARY_DENSITY_CODE = :SECONDARY_DENSITY_CODE,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint8(":SECONDARY_DENSITY_CODE", secondaryDensityCode);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint32_t> &nbWraps) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      NB_WRAPS = :NB_WRAPS,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint32(":NB_WRAPS", nbWraps);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &minLPos) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      MIN_LPOS = :MIN_LPOS,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":MIN_LPOS", minLPos);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &maxLPos) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      MAX_LPOS = :MAX_LPOS,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":MAX_LPOS", maxLPos);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

void RdbmsMediaTypeCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE MEDIA_TYPE SET 
      USER_COMMENT = :USER_COMMENT,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
  }
}

bool RdbmsMediaTypeCatalogue::mediaTypeIsUsedByTapes(rdbms::Conn &conn, const std::string &name) const {
  const char* const sql = R"SQL(
    SELECT 
      MEDIA_TYPE.MEDIA_TYPE_NAME 
    FROM 
      TAPE 
    INNER JOIN 
      MEDIA_TYPE 
    ON 
      TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID 
    WHERE 
      MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  auto rset = stmt.executeQuery();
  return rset.next();
}

std::optional<uint64_t> RdbmsMediaTypeCatalogue::getMediaTypeId(rdbms::Conn &conn, const std::string &name) const {
  const char* const sql = R"SQL(
    SELECT 
      MEDIA_TYPE.MEDIA_TYPE_ID AS MEDIA_TYPE_ID 
    FROM 
      MEDIA_TYPE 
    WHERE 
      MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    return std::nullopt;
  }
  return rset.columnUint64("MEDIA_TYPE_ID");
}

} // namespace cta::catalogue
