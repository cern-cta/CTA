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

#include <iostream>
#include <iomanip>
#include <cmdline/CtaAdminTextFormatter.hpp>
#include <common/checksum/ChecksumBlobSerDeser.hpp>
#include <common/dataStructures/DriveStatusSerDeser.hpp>
#include <common/dataStructures/MountTypeSerDeser.hpp>

#include "common/utils/utils.hpp"

namespace cta {
namespace admin {

/**
 ** Generic utility methods
 **/

std::string TextFormatter::doubleToStr(double value, char unit) {
  std::stringstream ss;
  ss << std::fixed << std::setprecision(1) << value;
  if(unit != '\0') ss << unit;
  return ss.str();
}


std::string TextFormatter::timeToStr(const time_t &unixtime) {
  struct tm timeTm;
  localtime_r(&unixtime, &timeTm);

  char timeStr[17]; // YYYY-MM-DD HH:MM
  strftime(timeStr, 17, "%F %R", &timeTm);

  return timeStr;
}

std::string TextFormatter::secondsToDayHoursMinSec(const uint64_t & seconds){
  uint64_t secondsSave = seconds;
  int day = secondsSave / (24 * 3600);

  secondsSave = secondsSave % (24 * 3600);
  int hour = secondsSave / 3600;

  secondsSave %= 3600;
  int minutes = secondsSave / 60 ;

  secondsSave %= 60;
  std::stringstream ss;
  if(day){
    ss << day << "d";
  }
  if(hour){
    ss << hour << "h";
  }
  if(minutes){
    ss << minutes << "m";
  }
  ss << secondsSave << "s";
  return ss.str();
}

std::string TextFormatter::integerToPercentage(const uint64_t & value){
  std::stringstream ss;
  ss << value << "%";
  return ss.str();
}


std::string TextFormatter::dataSizeToStr(uint64_t value) {
  const std::vector<char> suffix = { 'K', 'M', 'G', 'T', 'P', 'E' };

  // Simple case, values less than 1000 bytes don't take a suffix
  if(value < 1000) return std::to_string(value);

  // Find the correct scaling, starting at 1 KB and working up. I'm assuming we won't have zettabytes
  // or yottabytes of data in a tapepool anytime soon.
  int unit;
  uint64_t divisor;
  for(unit = 0, divisor = 1000; unit < 6 && value >= divisor*1000; divisor *= 1000, ++unit) ;

  // Convert to format like "3.1G"
  double val_d = static_cast<double>(value) / static_cast<double>(divisor);
  return doubleToStr(val_d, suffix[unit]);
}


void TextFormatter::flush() {
  if(m_outputBuffer.empty()) return;

  // Check if first line is a header requiring special formatting
  bool is_header = false;
  if(m_outputBuffer.front().size() == 1 && m_outputBuffer.front().front() == "HEADER") {
    is_header = true;
    m_outputBuffer.erase(m_outputBuffer.begin());
    if(m_outputBuffer.empty()) return;
    auto numCols = m_outputBuffer.front().size();
    m_colSize.resize(numCols);
  }

  // Calculate column widths
  for(auto &l : m_outputBuffer) {
    if(l.size() != m_colSize.size()) throw std::runtime_error("TextFormatter::flush(): incorrect number of columns");
    for(size_t c = 0; c < l.size(); ++c) {
      if(m_colSize.at(c) < l.at(c).size()) m_colSize[c] = l.at(c).size();
    }
  }

  // Output columns
  for(auto &l : m_outputBuffer) {
    if(is_header) { std::cout << TEXT_RED; }
    for(size_t c = 0; c < l.size(); ++c) {
      // flush right, except for comments, paths and drive reasons, which are flush left
      if(is_header && c == l.size()-1 &&
         (l.at(c) == "comment" || l.at(c) == "path" || l.at(c) == "reason")) {
        m_lastColumnFlushLeft = true;
      }

      auto flush = (c == l.size()-1 && m_lastColumnFlushLeft) ? std::left : std::right;

      std::cout << std::setfill(' ')
                << std::setw(m_colSize.at(c))
                << flush
                << (l.at(c).empty() ? "-" : l.at(c))
                << ' ';
    }
    if(is_header) { std::cout << TEXT_NORMAL; is_header = false; }
    std::cout << std::endl;
  }

  // Empty buffer
  m_outputBuffer.clear();
}


/**
 ** Output for specific commands
 **/

 void TextFormatter::printActivityMountRuleLsHeader() {
  push_back("HEADER");
  push_back(
    "instance",
    "username",
    "policy",
    "activity",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
 }

 void TextFormatter::print(const ActivityMountRuleLsItem &amrls_item) {
  push_back(
    amrls_item.disk_instance(),
    amrls_item.activity_mount_rule(),
    amrls_item.mount_policy(),
    amrls_item.activity_regex(),
    amrls_item.creation_log().username(),
    amrls_item.creation_log().host(),
    timeToStr(amrls_item.creation_log().time()),
    amrls_item.last_modification_log().username(),
    amrls_item.last_modification_log().host(),
    timeToStr(amrls_item.last_modification_log().time()),
    amrls_item.comment()
  );
}

void TextFormatter::printAdminLsHeader() {
  push_back("HEADER");
  push_back(
    "user",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const AdminLsItem &adls_item) {
  push_back(
    adls_item.user(),
    adls_item.creation_log().username(),
    adls_item.creation_log().host(),
    timeToStr(adls_item.creation_log().time()),
    adls_item.last_modification_log().username(),
    adls_item.last_modification_log().host(),
    timeToStr(adls_item.last_modification_log().time()),
    adls_item.comment()
  );
}

void TextFormatter::printArchiveRouteLsHeader() {
  push_back("HEADER");
  push_back(
    "storage class",
    "copy number",
    "tapepool",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const ArchiveRouteLsItem &arls_item) {
  push_back(
    arls_item.storage_class(),
    arls_item.copy_number(),
    arls_item.tapepool(),
    arls_item.creation_log().username(),
    arls_item.creation_log().host(),
    timeToStr(arls_item.creation_log().time()),
    arls_item.last_modification_log().username(),
    arls_item.last_modification_log().host(),
    timeToStr(arls_item.last_modification_log().time()),
    arls_item.comment()
  );
}


void TextFormatter::printDriveLsHeader() {
  push_back("HEADER");
  push_back(
    "library",
    "drive",
    "host",
    "desired",
    "request",
    "status",
    "since",
    "vid",
    "tapepool",
    "vo",
    "files",
    "data",
    "MB/s",
    "session",
    "priority",
    "activity",
    "age",
    "reason"
  );
}

void TextFormatter::print(const DriveLsItem &drls_item)
{
  //using namespace cta::common::dataStructures;

  const int DRIVE_TIMEOUT = 600; // Time after which a drive will be marked as STALE

  std::string driveStatusSince;
  std::string filesTransferredInSession;
  std::string bytesTransferredInSession;
  std::string averageBandwidth;
  std::string sessionId;
  std::string timeSinceLastUpdate;

  if(drls_item.drive_status() != DriveLsItem::UNKNOWN_DRIVE_STATUS) {
    driveStatusSince = std::to_string(drls_item.drive_status_since());
  }
  if(drls_item.drive_status() == DriveLsItem::TRANSFERRING) {
    filesTransferredInSession = std::to_string(drls_item.files_transferred_in_session());
    bytesTransferredInSession = dataSizeToStr(drls_item.bytes_transferred_in_session());
    if(drls_item.session_elapsed_time() > 0) {
      // Calculate average bandwidth for the session in MB/s (reported to 1 decimal place)
      double bandwidth = drls_item.bytes_transferred_in_session()/drls_item.session_elapsed_time();
      averageBandwidth = doubleToStr(bandwidth/1000000.0,'\0');
    } else {
      averageBandwidth = "0.0";
    }
  }
  if(drls_item.drive_status() != DriveLsItem::UP &&
     drls_item.drive_status() != DriveLsItem::DOWN &&
     drls_item.drive_status() != DriveLsItem::UNKNOWN_DRIVE_STATUS) {
    sessionId = std::to_string(drls_item.session_id());
  }

  timeSinceLastUpdate = std::to_string(drls_item.time_since_last_update()) +
    (drls_item.time_since_last_update() > DRIVE_TIMEOUT ? " [STALE]" : "");

  //If there is a reason, we only want to display the beginning
  std::string reason = cta::utils::postEllipsis(drls_item.reason(),NB_CHAR_REASON);
  const std::string mountType = toString(ProtobufToMountType(drls_item.mount_type()));

  push_back(
    drls_item.logical_library(),
    drls_item.drive_name(),
    drls_item.host(),
    (drls_item.desired_drive_state() == DriveLsItem::UP ? "Up" : "Down"),
    mountType != "NO_MOUNT" ? mountType : "-",
    toString(ProtobufToDriveStatus(drls_item.drive_status())),
    driveStatusSince,
    drls_item.vid(),
    drls_item.tapepool(),
    drls_item.vo(),
    filesTransferredInSession,
    bytesTransferredInSession,
    averageBandwidth,
    sessionId,
    drls_item.current_priority(),
    drls_item.current_activity(),
    timeSinceLastUpdate,
    reason
  );
}

void TextFormatter::printFailedRequestLsHeader() {
  push_back("HEADER");
  push_back(
    "object id",
    "request type",
    "copy no",
    "tapepool/vid",
    "requester",
    "group",
    "path"
  );
}

void TextFormatter::print(const FailedRequestLsItem &frls_item) {
  std::string request_type;
  std::string tapepool_vid;

  switch(frls_item.request_type()) {
    case admin::RequestType::ARCHIVE_REQUEST:
      request_type = "archive";
      tapepool_vid = frls_item.tapepool();
      break;
    case admin::RequestType::RETRIEVE_REQUEST:
      request_type = "retrieve";
      tapepool_vid = frls_item.tf().vid();
      break;
    default:
      throw std::runtime_error("Unrecognised request type: " + std::to_string(frls_item.request_type()));
  }

  push_back(
    frls_item.object_id(),
    request_type,
    frls_item.copy_nb(),
    tapepool_vid,
    frls_item.requester().username(),
    frls_item.requester().groupname(),
    frls_item.af().df().path()
  );

  // Note: failure log messages are available in frls_item.failurelogs(). These are not currently
  //       displayed in the text output, only in JSON.
}

void TextFormatter::printFailedRequestLsSummaryHeader() {
  push_back("HEADER");
  push_back(
    "request type",
    "total files",
    "total size"
  );
}

void TextFormatter::print(const FailedRequestLsSummary &frls_summary) {
  std::string request_type =
    frls_summary.request_type() == RequestType::ARCHIVE_REQUEST  ? "archive" :
    frls_summary.request_type() == RequestType::RETRIEVE_REQUEST ? "retrieve" : "total";

  push_back(
    request_type,
    frls_summary.total_files(),
    dataSizeToStr(frls_summary.total_size())
  );
}

void TextFormatter::printGroupMountRuleLsHeader() {
  push_back("HEADER");
  push_back(
    "instance",
    "group",
    "policy",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const GroupMountRuleLsItem &gmrls_item) {
  push_back(
    gmrls_item.disk_instance(),
    gmrls_item.group_mount_rule(),
    gmrls_item.mount_policy(),
    gmrls_item.creation_log().username(),
    gmrls_item.creation_log().host(),
    timeToStr(gmrls_item.creation_log().time()),
    gmrls_item.last_modification_log().username(),
    gmrls_item.last_modification_log().host(),
    timeToStr(gmrls_item.last_modification_log().time()),
    gmrls_item.comment()
  );
}

void TextFormatter::printListPendingArchivesHeader() {
  push_back("HEADER");
  push_back(
    "tapepool",
    "archive id",
    "storage class",
    "copy no",
    "disk id",
    "instance",
    "checksum type",
    "checksum value",
    "size",
    "user",
    "group",
    "path"
  );
}

void TextFormatter::print(const ListPendingArchivesItem &lpa_item) {
  using namespace cta::checksum;

  std::string checksumType("NONE");
  std::string checksumValue;

  ChecksumBlob csb;
  ProtobufToChecksumBlob(lpa_item.af().csb(), csb);

  // Files can have multiple checksums of different types. Display only the first checksum here. All
  // checksums will be listed in JSON.
  if(!csb.empty()) {
    auto cs_it = csb.getMap().begin();
    checksumType = ChecksumTypeName.at(cs_it->first);
    checksumValue = "0x" + ChecksumBlob::ByteArrayToHex(cs_it->second);
  }

  push_back(
    lpa_item.tapepool(),
    lpa_item.af().archive_id(),
    lpa_item.af().storage_class(),
    lpa_item.copy_nb(),
    lpa_item.af().disk_id(),
    lpa_item.af().disk_instance(),
    checksumType,
    checksumValue,
    dataSizeToStr(lpa_item.af().size()),
    lpa_item.af().df().owner_id().uid(),
    lpa_item.af().df().owner_id().gid(),
    lpa_item.af().df().path()
  );
}

void TextFormatter::printListPendingArchivesSummaryHeader() {
  push_back("HEADER");
  push_back(
    "tapepool",
    "total files",
    "total size"
  );
}

void TextFormatter::print(const ListPendingArchivesSummary &lpa_summary) {
  push_back(
    lpa_summary.tapepool(),
    lpa_summary.total_files(),
    dataSizeToStr(lpa_summary.total_size())
  );
}

void TextFormatter::printListPendingRetrievesHeader() {
  push_back("HEADER");
  push_back(
    "vid",
    "archive id",
    "copy no",
    "fseq",
    "block id",
    "size",
    "user",
    "group",
    "path"
  );
}

void TextFormatter::print(const ListPendingRetrievesItem &lpr_item) {
  push_back(
    lpr_item.tf().vid(),
    lpr_item.af().archive_id(),
    lpr_item.copy_nb(),
    lpr_item.tf().f_seq(),
    lpr_item.tf().block_id(),
    dataSizeToStr(lpr_item.af().size()),
    lpr_item.af().df().owner_id().uid(),
    lpr_item.af().df().owner_id().gid(),
    lpr_item.af().df().path()
  );
}

void TextFormatter::printListPendingRetrievesSummaryHeader() {
  push_back("HEADER");
  push_back(
    "vid",
    "total files",
    "total size"
  );
}

void TextFormatter::print(const ListPendingRetrievesSummary &lpr_summary) {
  push_back(
    lpr_summary.vid(),
    lpr_summary.total_files(),
    dataSizeToStr(lpr_summary.total_size())
  );
}

void TextFormatter::printLogicalLibraryLsHeader() {
  push_back("HEADER");
  push_back(
    "library",
    "disabled",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const LogicalLibraryLsItem &llls_item) {
  push_back(
    llls_item.name(),
    llls_item.is_disabled(),
    llls_item.creation_log().username(),
    llls_item.creation_log().host(),
    timeToStr(llls_item.creation_log().time()),
    llls_item.last_modification_log().username(),
    llls_item.last_modification_log().host(),
    timeToStr(llls_item.last_modification_log().time()),
    llls_item.comment()
  );
}

void TextFormatter::printMediaTypeLsHeader() {
  push_back("HEADER");
  push_back(
    "media type",
    "cartridge",
    "capacity",
    "primary density code",
    "secondary density code",
    "number of wraps",
    "min LPos",
    "max LPos",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const MediaTypeLsItem &mtls_item) {
  push_back(
    mtls_item.name(),
    mtls_item.cartridge(),
    mtls_item.capacity(),
    mtls_item.primary_density_code(),
    mtls_item.secondary_density_code(),
    mtls_item.number_of_wraps(),
    mtls_item.min_lpos(),
    mtls_item.max_lpos(),
    mtls_item.creation_log().username(),
    mtls_item.creation_log().host(),
    timeToStr(mtls_item.creation_log().time()),
    mtls_item.last_modification_log().username(),
    mtls_item.last_modification_log().host(),
    timeToStr(mtls_item.last_modification_log().time()),
    mtls_item.comment()
  );
}

void TextFormatter::printMountPolicyLsHeader() {
  push_back("HEADER");
  push_back(
    "mount policy",
    "a.priority",
    "a.minAge",
    "r.priority",
    "r.minAge",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const MountPolicyLsItem &mpls_item) {
  push_back(
    mpls_item.name(),
    mpls_item.archive_priority(),
    mpls_item.archive_min_request_age(),
    mpls_item.retrieve_priority(),
    mpls_item.retrieve_min_request_age(),
    mpls_item.creation_log().username(),
    mpls_item.creation_log().host(),
    timeToStr(mpls_item.creation_log().time()),
    mpls_item.last_modification_log().username(),
    mpls_item.last_modification_log().host(),
    timeToStr(mpls_item.last_modification_log().time()),
    mpls_item.comment()
  );
}

void TextFormatter::printRepackLsHeader() {
  push_back("HEADER");
  push_back(
    "c.time",
    "repackTime",
    "c.user",
    "vid",
    "tapepool",
    "providedFiles",
    "totalFiles",
    "totalBytes",
    "filesToRetrieve",
    "filesToArchive",
    "failed",
    "status"
  );
}

void TextFormatter::print(const RepackLsItem &rels_item) {
 push_back(
   timeToStr(rels_item.creation_log().time()),
   secondsToDayHoursMinSec(rels_item.repack_time()),
   rels_item.creation_log().username(),
   rels_item.vid(),
   rels_item.tapepool(),
   rels_item.user_provided_files(),
   rels_item.total_files_to_archive(), //https://gitlab.cern.ch/cta/CTA/-/issues/680#note_3849045
   dataSizeToStr(rels_item.total_bytes_to_archive()), //https://gitlab.cern.ch/cta/CTA/-/issues/680#note_3849045
   rels_item.files_left_to_retrieve(), //https://gitlab.cern.ch/cta/CTA/-/issues/680#note_3845829
   rels_item.files_left_to_archive(), //https://gitlab.cern.ch/cta/CTA/-/issues/680#note_3845829
   rels_item.total_failed_files(),  //https://gitlab.cern.ch/cta/CTA/-/issues/680#note_3849927
   rels_item.status()
  );
}

void TextFormatter::printRequesterMountRuleLsHeader() {
  push_back("HEADER");
  push_back(
    "instance",
    "username",
    "policy",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const RequesterMountRuleLsItem &rmrls_item) {
  push_back(
    rmrls_item.disk_instance(),
    rmrls_item.requester_mount_rule(),
    rmrls_item.mount_policy(),
    rmrls_item.creation_log().username(),
    rmrls_item.creation_log().host(),
    timeToStr(rmrls_item.creation_log().time()),
    rmrls_item.last_modification_log().username(),
    rmrls_item.last_modification_log().host(),
    timeToStr(rmrls_item.last_modification_log().time()),
    rmrls_item.comment()
  );
}

void TextFormatter::printShowQueuesHeader() {
  push_back("HEADER");
  push_back(
    "type",
    "tapepool",
    "vo",
    "library",
    "vid",
    "files queued",
    "data queued",
    "oldest",
    "youngest",
    "priority",
    "min age",
    "read max drives",
    "write max drives",
    "cur. mounts",
    "cur. files",
    "cur. data",
    "MB/s",
    "tapes capacity",
    "files on tapes",
    "data on tapes",
    "full tapes",
    "writable tapes"
  );
}

void TextFormatter::print(const ShowQueuesItem &sq_item) {
  std::string priority;
  std::string minAge;
  std::string readMaxDrives;
  std::string writeMaxDrives;

  if(sq_item.mount_type() == ARCHIVE_FOR_USER || sq_item.mount_type() == ARCHIVE_FOR_REPACK ||
     sq_item.mount_type() == RETRIEVE) {
    priority         = std::to_string(sq_item.priority());
    minAge           = std::to_string(sq_item.min_age());
    readMaxDrives = std::to_string(sq_item.read_max_drives());
    writeMaxDrives = std::to_string(sq_item.write_max_drives());
  }

  push_back(
    toString(ProtobufToMountType(sq_item.mount_type())),
    sq_item.tapepool(),
    sq_item.vo(),
    sq_item.logical_library(),
    sq_item.vid(),
    sq_item.queued_files(),
    dataSizeToStr(sq_item.queued_bytes()),
    sq_item.oldest_age(),
    sq_item.youngest_age(),
    priority,
    minAge,
    readMaxDrives,
    writeMaxDrives,
    sq_item.cur_mounts(),
    sq_item.cur_files(),
    dataSizeToStr(sq_item.cur_bytes()),
    sq_item.bytes_per_second() / 1000000,
    dataSizeToStr(sq_item.tapes_capacity()),
    sq_item.tapes_files(),
    dataSizeToStr(sq_item.tapes_bytes()),
    sq_item.full_tapes(),
    sq_item.writable_tapes()
  );
}

void TextFormatter::printStorageClassLsHeader() {
  push_back("HEADER");
  push_back(
    "storage class",
    "number of copies",
    "vo",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const StorageClassLsItem &scls_item) {
  push_back(
    scls_item.name(),
    scls_item.nb_copies(),
    scls_item.vo(),
    scls_item.creation_log().username(),
    scls_item.creation_log().host(),
    timeToStr(scls_item.creation_log().time()),
    scls_item.last_modification_log().username(),
    scls_item.last_modification_log().host(),
    timeToStr(scls_item.last_modification_log().time()),
    scls_item.comment()
  );
}

void TextFormatter::printTapeLsHeader() {
  push_back("HEADER");
  push_back(
    "vid",
    "media type",
    "vendor",
    "library",
    "tapepool",
    "vo",
    "encryption key name",
    "capacity",
    "occupancy",
    "last fseq",
    "full",
    "from castor",
    "state",
    "state reason",
    "label drive",
    "label time",
    "last w drive",
    "last w time",
    "w mounts",
    "last r drive",
    "last r time",
    "r mounts",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const TapeLsItem &tals_item) {
  std::string state_reason = cta::utils::postEllipsis(tals_item.state_reason(),NB_CHAR_REASON);
  push_back(
    tals_item.vid(),
    tals_item.media_type(),
    tals_item.vendor(),
    tals_item.logical_library(),
    tals_item.tapepool(),
    tals_item.vo(),
    tals_item.encryption_key_name(),
    dataSizeToStr(tals_item.capacity()),
    dataSizeToStr(tals_item.occupancy()),
    tals_item.last_fseq(),
    tals_item.full(),
    tals_item.from_castor(),
    tals_item.state(),
    state_reason,
    tals_item.has_label_log()        ? tals_item.label_log().drive()                  : "",
    tals_item.has_label_log()        ? timeToStr(tals_item.label_log().time())        : "",
    tals_item.has_last_written_log() ? tals_item.last_written_log().drive()           : "",
    tals_item.has_last_written_log() ? timeToStr(tals_item.last_written_log().time()) : "",
    tals_item.write_mount_count(),
    tals_item.has_last_read_log()    ? tals_item.last_read_log().drive()              : "",
    tals_item.has_last_read_log()    ? timeToStr(tals_item.last_read_log().time())    : "",
    tals_item.read_mount_count(),
    tals_item.creation_log().username(),
    tals_item.creation_log().host(),
    timeToStr(tals_item.creation_log().time()),
    tals_item.last_modification_log().username(),
    tals_item.last_modification_log().host(),
    timeToStr(tals_item.last_modification_log().time()),
    tals_item.comment()
  );
}

void TextFormatter::printTapeFileLsHeader() {
  push_back("HEADER");
  push_back(
    "archive id",
    "copy no",
    "vid",
    "fseq",
    "block id",
    "instance",
    "disk fxid",
    "size",
    "checksum type",
    "checksum value",
    "storage class",
    "owner",
    "group",
    "creation time",
    "path"
  );
}

void TextFormatter::print(const TapeFileLsItem &tfls_item) {
  // Files can have multiple checksums of different types. The tabular output will
  // display only the first checksum; the JSON output will list all checksums.
  std::string checksumType("NONE");
  std::string checksumValue;

  if(!tfls_item.af().checksum().empty()) {
    const google::protobuf::EnumDescriptor *descriptor = cta::common::ChecksumBlob::Checksum::Type_descriptor();
    checksumType  = descriptor->FindValueByNumber(tfls_item.af().checksum().begin()->type())->name();
    checksumValue = tfls_item.af().checksum().begin()->value();
  }
  auto fid = strtol(tfls_item.df().disk_id().c_str(), nullptr, 10);
  std::stringstream fxid;
  fxid << std::hex << fid;

  push_back(
    tfls_item.af().archive_id(),
    tfls_item.tf().copy_nb(),
    tfls_item.tf().vid(),
    tfls_item.tf().f_seq(),
    tfls_item.tf().block_id(),
    tfls_item.df().disk_instance(),
    fxid.str(),
    dataSizeToStr(tfls_item.af().size()),
    checksumType,
    checksumValue,
    tfls_item.af().storage_class(),
    tfls_item.df().owner_id().uid(),
    tfls_item.df().owner_id().gid(),
    timeToStr(tfls_item.af().creation_time()),
    tfls_item.df().path()
  );
}

void TextFormatter::printTapePoolLsHeader() {
  push_back("HEADER");
  push_back(
    "name",
    "vo",
    "#tapes",
    "#partial",
    "#phys files",
    "size",
    "used",
    "avail",
    "use%",
    "encrypt",
    "supply",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const TapePoolLsItem &tpls_item)
{
  uint64_t avail = tpls_item.capacity_bytes() > tpls_item.data_bytes() ?
    tpls_item.capacity_bytes()-tpls_item.data_bytes() : 0;

  double use_percent = tpls_item.capacity_bytes() > 0 ?
    (static_cast<double>(tpls_item.data_bytes())/static_cast<double>(tpls_item.capacity_bytes()))*100.0 : 0.0;

  push_back(
    tpls_item.name(),
    tpls_item.vo(),
    tpls_item.num_tapes(),
    tpls_item.num_partial_tapes(),
    tpls_item.num_physical_files(),
    dataSizeToStr(tpls_item.capacity_bytes()),
    dataSizeToStr(tpls_item.data_bytes()),
    dataSizeToStr(avail),
    doubleToStr(use_percent, '%'),
    tpls_item.encrypt(),
    tpls_item.supply(),
    tpls_item.created().username(),
    tpls_item.created().host(),
    timeToStr(tpls_item.created().time()),
    tpls_item.modified().username(),
    tpls_item.modified().host(),
    timeToStr(tpls_item.modified().time()),
    tpls_item.comment()
  );
}

void TextFormatter::printDiskSystemLsHeader() {
  push_back("HEADER");
  push_back(
    "name",
    "regexp",
    "url",
    "refresh",
    "space",
    "sleep",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::printDiskInstanceLsHeader() {
  push_back("HEADER");
  push_back(
    "name",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::printDiskInstanceSpaceLsHeader() {
  push_back("HEADER");
  push_back(
    "name",
    "instance",
    "url",
    "interval",
    "last refresh",
    "space",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const DiskInstanceSpaceLsItem &disls_item)
{
  push_back(
    disls_item.name(),
    disls_item.disk_instance(),
    disls_item.free_space_query_url(),
    disls_item.refresh_interval(),
    disls_item.last_refresh_time(),
    disls_item.free_space(),
    disls_item.creation_log().username(),
    disls_item.creation_log().host(),
    timeToStr(disls_item.creation_log().time()),
    disls_item.last_modification_log().username(),
    disls_item.last_modification_log().host(),
    timeToStr(disls_item.last_modification_log().time()),
    disls_item.comment()
  );
}

void TextFormatter::print(const DiskInstanceLsItem &dils_item)
{
  push_back(
    dils_item.name(),
    dils_item.creation_log().username(),
    dils_item.creation_log().host(),
    timeToStr(dils_item.creation_log().time()),
    dils_item.last_modification_log().username(),
    dils_item.last_modification_log().host(),
    timeToStr(dils_item.last_modification_log().time()),
    dils_item.comment()
  );
}

void TextFormatter::print(const DiskSystemLsItem &dsls_item)
{
  push_back(
    dsls_item.name(),
    dsls_item.file_regexp(),
    dsls_item.free_space_query_url(),
    dsls_item.refresh_interval(),
    dsls_item.targeted_free_space(),
    dsls_item.sleep_time(),
    dsls_item.creation_log().username(),
    dsls_item.creation_log().host(),
    timeToStr(dsls_item.creation_log().time()),
    dsls_item.last_modification_log().username(),
    dsls_item.last_modification_log().host(),
    timeToStr(dsls_item.last_modification_log().time()),
    dsls_item.comment()
  );
}

void TextFormatter::printVirtualOrganizationLsHeader(){
  push_back("HEADER");
  push_back(
    "name",
    "read max drives",
    "write max drives",
    "max file size",
    "c.user",
    "c.host",
    "c.time",
    "m.user",
    "m.host",
    "m.time",
    "comment"
  );
}

void TextFormatter::print(const VirtualOrganizationLsItem& vols_item){
  push_back(
    vols_item.name(),
    vols_item.read_max_drives(),
    vols_item.write_max_drives(),
    dataSizeToStr(vols_item.max_file_size()),
    vols_item.creation_log().username(),
    vols_item.creation_log().host(),
    timeToStr(vols_item.creation_log().time()),
    vols_item.last_modification_log().username(),
    vols_item.last_modification_log().host(),
    timeToStr(vols_item.last_modification_log().time()),
    vols_item.comment()
  );
}

void TextFormatter::printVersionHeader(){
  push_back("HEADER");
  push_back(
    "CTA Admin",
    "Client xrd-ssi-protobuf",
    "CTA Frontend",
    "Server xrd-ssi-protobuf",
    "Catalogue schema",
    "DB connection string"
  );
}

void TextFormatter::print(const VersionItem & version_item){
  push_back(
    version_item.client_version().cta_version(),
    version_item.client_version().xrootd_ssi_protobuf_interface_version(),
    version_item.server_version().cta_version(),
    version_item.server_version().xrootd_ssi_protobuf_interface_version(),
    version_item.catalogue_version(),
    version_item.catalogue_connection_string()
  );
}

void TextFormatter::printSchedulingInfosLsHeader(){
  push_back("HEADER");
  push_back("No tabular output available for this command, please use the --json flag.");
}

void TextFormatter::printRecycleTapeFileLsHeader() {
  push_back("HEADER");
  push_back(
    "archive id",
    "copy no",
    "vid",
    "fseq",
    "block id",
    "instance",
    "disk fxid",
    "size",
    "checksum type",
    "checksum value",
    "storage class",
    "owner",
    "group",
    "deletion time",
    "path when deleted",
    "reason"
  );
}

void TextFormatter::print(const RecycleTapeFileLsItem & rtfls_item){
  // Files can have multiple checksums of different types. The tabular output will
  // display only the first checksum; the JSON output will list all checksums.
  std::string checksumType("NONE");
  std::string checksumValue;

  if(!rtfls_item.checksum().empty()) {
    const google::protobuf::EnumDescriptor *descriptor = cta::common::ChecksumBlob::Checksum::Type_descriptor();
    checksumType  = descriptor->FindValueByNumber(rtfls_item.checksum().begin()->type())->name();
    checksumValue = rtfls_item.checksum().begin()->value();
  }
  auto fid = strtol(rtfls_item.disk_file_id().c_str(), nullptr, 10);
  std::stringstream fxid;
  fxid << std::hex << fid;

  push_back(
    rtfls_item.archive_file_id(),
    rtfls_item.copy_nb(),
    rtfls_item.vid(),
    rtfls_item.fseq(),
    rtfls_item.block_id(),
    rtfls_item.disk_instance(),
    fxid.str(),
    dataSizeToStr(rtfls_item.size_in_bytes()),
    checksumType,
    checksumValue,
    rtfls_item.storage_class(),
    rtfls_item.disk_file_uid(),
    rtfls_item.disk_file_gid(),
    timeToStr(rtfls_item.recycle_log_time()),
    rtfls_item.disk_file_path(),
    rtfls_item.reason_log()
  );
}

}}
