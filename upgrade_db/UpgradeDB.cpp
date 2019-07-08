/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Migration tool to upgrade Oracle DB
 * @copyright      Copyright 2019 CERN
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

#include <iostream>
#include <sstream>
#include <vector>

#include <XrdSsiPbConfig.hpp>
#include <common/exception/Exception.hpp>
#include <common/checksum/ChecksumBlob.hpp>
#include "OracleDbConn.hpp"

namespace cta {
namespace migration {

//! DB Connection Pool
std::unique_ptr<rdbms::ConnPool> OracleDbConn::m_connPool;

//! Mapping of users to uid
const std::map<std::string,uint32_t> UidMap = {
  { "atlas003",  10763 },
  { "cmsrobot", 109701 },
  { "mdavis",    71761 },
  { "ctaops",    98119 }
};

//! Mapping of groups to gid
const std::map<std::string,uint32_t> GidMap = {
  { "zp",     1307 },
  { "def-cg", 2766 },
  { "si",     1077 },
  { "it",     2763 }
};


class UpgradeDB {
public:
  UpgradeDB(const std::string &configfile);

  void renameLogical();
  void undoRenameLogical();
  void addUidGid();
  void removeUidGid();
  void addChecksumBlob();
  void populateChecksumBlob();
  void removeChecksumBlob();
  void addAdler32();
  void removeAdler32();

private:
  void populateAdler32();

  OracleDbConn m_ctadb;         //!< Oracle database for CTA Catalogue
  unsigned int m_max_depth;     //!< Maximum directory tree depth to import
  unsigned int m_cur_depth;     //!< Current directory tree depth
  unsigned int m_batch_size;    //!< Number of records to fetch from the DB at a time
};



UpgradeDB::UpgradeDB(const std::string &configfile) {
  // Parse configuration file
  XrdSsiPb::Config config(configfile);

  auto dbconn        = config.getOptionValueStr("cta.db_login");
  auto max_num_conns = config.getOptionValueInt("cta.max_num_connections");
  auto batch_size    = config.getOptionValueInt("cta.batch_size");

  // Connect to Oracle
  if(!dbconn.first) {
    throw std::runtime_error("cta.db_login must be specified in the config file in the form oracle:user/password@TNS");
  }
  m_ctadb.connect(dbconn.second, max_num_conns.first ? max_num_conns.second : 1);

  // Set parameters and defaults
  m_batch_size = batch_size.first ? batch_size.second : 1000;
}


void UpgradeDB::renameLogical() {
  std::cerr << "Renaming column COMPRESSED_SIZE_IN_BYTES to LOGICAL_SIZE_IN_BYTES in TAPE_FILE table...";
  m_ctadb.execute("ALTER TABLE TAPE_FILE RENAME COLUMN COMPRESSED_SIZE_IN_BYTES TO LOGICAL_SIZE_IN_BYTES");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::undoRenameLogical() {
  std::cerr << "Renaming column LOGICAL_SIZE_IN_BYTES to COMPRESSED_SIZE_IN_BYTES in TAPE_FILE table...";
  m_ctadb.execute("ALTER TABLE TAPE_FILE RENAME COLUMN LOGICAL_SIZE_IN_BYTES TO COMPRESSED_SIZE_IN_BYTES");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::addUidGid() {
  std::cerr << "Adding DISK_FILE_UID and DISK_FILE_GID columns to ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE ADD ("
    "DISK_FILE_UID NUMERIC(20, 0),"
    "DISK_FILE_GID NUMERIC(20, 0))");
  std::cerr << "done." << std::endl;

  // Update UIDs
  std::cerr << "Populating DISK_FILE_UID";
  for(auto it = UidMap.begin(); it != UidMap.end(); ++it) {
    std::cerr << "...";
    std::string sql("UPDATE ARCHIVE_FILE SET DISK_FILE_UID=");
    sql += std::to_string(it->second) + " WHERE DISK_FILE_USER='" + it->first + "'";
    m_ctadb.execute(sql);
  }
  std::cerr << "done." << std::endl;

  // Update GIDs
  std::cerr << "Populating DISK_FILE_GID";
  for(auto it = GidMap.begin(); it != GidMap.end(); ++it) {
    std::cerr << "...";
    std::string sql("UPDATE ARCHIVE_FILE SET DISK_FILE_GID=");
    sql += std::to_string(it->second) + " WHERE DISK_FILE_GROUP='" + it->first + "'";
    m_ctadb.execute(sql);
  }
  std::cerr << "done." << std::endl;

  std::cerr << "Adding constraints to ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE MODIFY (DISK_FILE_UID NUMERIC(20, 0) CONSTRAINT ARCHIVE_FILE_DFUID_NN NOT NULL)");
  std::cerr << "...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE MODIFY (DISK_FILE_GID NUMERIC(20, 0) CONSTRAINT ARCHIVE_FILE_DFGID_NN NOT NULL)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::removeUidGid() {
  std::cerr << "Removing DISK_FILE_UID and DISK_FILE_GID columns from ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE DROP (DISK_FILE_UID, DISK_FILE_GID)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::addChecksumBlob() {
  std::cerr << "Adding column CHECKSUM_BLOB in ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE ADD (CHECKSUM_BLOB RAW(200))");
  std::cerr << "done." << std::endl;

  std::cerr << "Initialising CHECKSUM_BLOB to invalid values...";
  std::string CASTOR = checksum::ChecksumBlob::ByteArrayToHex("CASTOR");
  m_ctadb.execute("UPDATE ARCHIVE_FILE SET CHECKSUM_BLOB = '" + CASTOR + "'");
  std::cerr << "done." << std::endl;

#if 0
  // We should only be dealing with ADLER32 checksums
  m_ctadb.query("SELECT CHECKSUM_TYPE, COUNT(*) AS CNT FROM ARCHIVE_FILE GROUP BY CHECKSUM_TYPE");
  if(!m_ctadb.isQueryEmpty()) {
    auto checksumType = m_ctadb.getResultColumnString("CHECKSUM_TYPE");
    std::cerr << "Updating " << m_ctadb.getResultColumnString("CNT") << " checksums of type " << checksumType << "...";
    if(checksumType != "ADLER32") throw std::runtime_error("Checksum type is not ADLER32, aborting");
    populateChecksumBlob();
    std::cerr << "done." << std::endl;
  }
#endif

  std::cerr << "Adding constraint to ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE MODIFY (CHECKSUM_BLOB RAW(200) CONSTRAINT ARCHIVE_FILE_CB1_NN NOT NULL)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::removeChecksumBlob() {
  std::cerr << "Removing column CHECKSUM_BLOB from ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE DROP (CHECKSUM_BLOB)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::populateChecksumBlob() {
  m_ctadb.query("SELECT ARCHIVE_FILE_ID, CHECKSUM_VALUE FROM ARCHIVE_FILE");

  // Get the list of checksums
  while(!m_ctadb.isQueryEmpty()) {
    using namespace checksum;

    auto archiveFileId = m_ctadb.getResultColumnString("ARCHIVE_FILE_ID");
    auto checksumValue = m_ctadb.getResultColumnString("CHECKSUM_VALUE");
    ChecksumBlob csb(ADLER32, ChecksumBlob::HexToByteArray(checksumValue));

    std::string blob_str;
    std::string inv_blob_str(ChecksumBlob::ByteArrayToHex(csb.serialize()));
    for(unsigned int i = 0; i < inv_blob_str.length(); i += 2) {
      blob_str = inv_blob_str.substr(i,2) + blob_str;
    }
    std::string sql("UPDATE ARCHIVE_FILE SET CHECKSUM_BLOB = hextoraw('");
    sql += blob_str + "') WHERE ARCHIVE_FILE_ID = " + archiveFileId;
    m_ctadb.execute(sql);

    if(!m_ctadb.nextRow()) break;
  }

  // Validate checksums
  m_ctadb.query("SELECT CHECKSUM_VALUE, CHECKSUM_BLOB FROM ARCHIVE_FILE");
  while(!m_ctadb.isQueryEmpty()) {
    using namespace checksum;

    auto checksumValue = m_ctadb.getResultColumnString("CHECKSUM_VALUE");
    auto checksumBlob  = m_ctadb.getResultColumnBlob("CHECKSUM_BLOB");
    ChecksumBlob csb1(ADLER32, ChecksumBlob::HexToByteArray(checksumValue));
    ChecksumBlob csb2;
    csb2.deserialize(checksumBlob);
    csb2.validate(csb1);

    if(!m_ctadb.nextRow()) break;
  }
}

void UpgradeDB::addAdler32() {
  std::cerr << "Adding column CHECKSUM_ADLER32 in ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE ADD (CHECKSUM_ADLER32 NUMERIC(20,0))");
  std::cerr << "done." << std::endl;

  // We should only be dealing with ADLER32 checksums
  m_ctadb.query("SELECT CHECKSUM_TYPE, COUNT(*) AS CNT FROM ARCHIVE_FILE GROUP BY CHECKSUM_TYPE");
  if(!m_ctadb.isQueryEmpty()) {
    auto checksumType = m_ctadb.getResultColumnString("CHECKSUM_TYPE");
    std::cerr << "Updating " << m_ctadb.getResultColumnString("CNT") << " checksums of type " << checksumType << "...";
    if(checksumType != "ADLER32") throw std::runtime_error("Checksum type is not ADLER32, aborting");
    populateAdler32();
    std::cerr << "done." << std::endl;
  }

  std::cerr << "Adding constraint to ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE MODIFY (CHECKSUM_ADLER32 NUMERIC(20,0) CONSTRAINT ARCHIVE_FILE_CB2_NN NOT NULL)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::removeAdler32() {
  std::cerr << "Removing column CHECKSUM_ADLER32 from ARCHIVE_FILE table...";
  m_ctadb.execute("ALTER TABLE ARCHIVE_FILE DROP (CHECKSUM_ADLER32)");
  std::cerr << "done." << std::endl;
}

void UpgradeDB::populateAdler32() {
  m_ctadb.query("SELECT ARCHIVE_FILE_ID, CHECKSUM_VALUE FROM ARCHIVE_FILE");

  // Get the list of checksums
  while(!m_ctadb.isQueryEmpty()) {
    using namespace checksum;

    auto archiveFileId = m_ctadb.getResultColumnString("ARCHIVE_FILE_ID");
    auto checksumValue = m_ctadb.getResultColumnString("CHECKSUM_VALUE");
    uint32_t checksumAdler32 = strtoul(checksumValue.c_str(), 0, 16);

    std::string sql("UPDATE ARCHIVE_FILE SET CHECKSUM_ADLER32 = ");
    sql += std::to_string(checksumAdler32) + " WHERE ARCHIVE_FILE_ID = " + archiveFileId;
    m_ctadb.execute(sql);

    if(!m_ctadb.nextRow()) break;
  }

  // Validate checksums
  m_ctadb.query("SELECT CHECKSUM_VALUE, CHECKSUM_ADLER32 FROM ARCHIVE_FILE");
  while(!m_ctadb.isQueryEmpty()) {
    using namespace checksum;

    auto checksumValue = m_ctadb.getResultColumnString("CHECKSUM_VALUE");
    auto adler32str = m_ctadb.getResultColumnString("CHECKSUM_ADLER32");
    uint32_t checksumAdler32 = strtoul(adler32str.c_str(), 0, 10);
    ChecksumBlob csb1(ADLER32, ChecksumBlob::HexToByteArray(checksumValue));
    ChecksumBlob csb2(ADLER32, checksumAdler32);
    csb2.validate(csb1);

    if(!m_ctadb.nextRow()) break;
  }
}

}} // namespace cta::migration


void throwUsage(const std::string &program, const std::string &error_txt)
{
  std::stringstream help;

  help << program << ": " << error_txt << std::endl
       << "Usage: " << program << " [--config <config_file>] [--rename-logical] [--undo-rename-logical] [--add-uid-gid] [--remove-uid-gid] [--add-checksum-blob] [--populate-checksum-blob] [--remove-checksum-blob] [--add-adler32] [--remove-adler32]";

  throw std::runtime_error(help.str());
}


int main(int argc, const char* argv[])
{
  std::string configfile = "/etc/cta/cta-catalogue-upgrade.conf";

  bool renameLogical = false;
  bool undoRenameLogical = false;
  bool addUidGid = false;
  bool removeUidGid = false;
  bool addChecksumBlob = false;
  bool populateChecksumBlob = false;
  bool removeChecksumBlob = false;
  bool addAdler32 = false;
  bool removeAdler32 = false;

  try {
    // Parse options
    if(argc < 2) throwUsage(argv[0], "");
    for(auto i = 1; i < argc; ++i) {
      std::string option(argv[i]);

           if(option == "--config" && argc > ++i) configfile = argv[i];
      else if(option == "--rename-logical") renameLogical = true;
      else if(option == "--undo-rename-logical") undoRenameLogical = true;
      else if(option == "--add-uid-gid") addUidGid = true;
      else if(option == "--remove-uid-gid") removeUidGid = true;
      else if(option == "--add-checksum-blob") addChecksumBlob = true;
      else if(option == "--populate-checksum-blob") populateChecksumBlob = true;
      else if(option == "--remove-checksum-blob") removeChecksumBlob = true;
      else if(option == "--add-adler32") addAdler32 = true;
      else if(option == "--remove-adler32") removeAdler32 = true;
      else throwUsage(argv[0], "invalid option " + option);
    }

    // Process options
    cta::migration::UpgradeDB ctaDb(configfile);

    if(renameLogical) ctaDb.renameLogical();
    if(undoRenameLogical) ctaDb.undoRenameLogical();
    if(addUidGid) ctaDb.addUidGid();
    if(removeUidGid) ctaDb.removeUidGid();
    if(addChecksumBlob) ctaDb.addChecksumBlob();
    if(populateChecksumBlob) ctaDb.populateChecksumBlob();
    if(removeChecksumBlob) ctaDb.removeChecksumBlob();
    if(addAdler32) ctaDb.addAdler32();
    if(removeAdler32) ctaDb.removeAdler32();
  } catch(cta::exception::Exception &ex) {
    std::cerr << ex.getMessage().str() << std::endl;
  } catch(std::runtime_error &ex) {
    std::cerr << ex.what() << std::endl;
    return -1;
  }
  return 0;
}
