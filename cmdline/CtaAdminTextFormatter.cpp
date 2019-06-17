/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Text formatter for CTA Admin command tool
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
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


namespace cta { namespace admin {

const std::string TextFormatter::TEXT_RED    = "\x1b[31;1m";
const std::string TextFormatter::TEXT_NORMAL = "\x1b[0m";

/*
 * Convert time to string
 *
 * NOTE: ctime is not thread-safe!
 */
std::string timeToString(const time_t &time)
{
  std::string timeString(ctime(&time));
  timeString.resize(timeString.size()-1); //remove newline
  return timeString;
}


void TextFormatter::flush() {
  if(m_outputBuffer.empty()) return;

  auto numCols = m_outputBuffer.front().size();
  std::vector<unsigned int> colSize(numCols);

  // Calculate column widths
  for(auto &l : m_outputBuffer) {
    if(l.size() != numCols) throw std::runtime_error("TextFormatter::flush(): incorrect number of columns");
    for(size_t c = 0; c < l.size(); ++c) {
      if(colSize.at(c) < l.at(c).size()) colSize[c] = l.at(c).size();
    }
  }

  // Output columns
  for(auto &l : m_outputBuffer) {
    for(size_t c = 0; c < l.size(); ++c) {
      std::cout << std::setfill(' ')
                << std::setw(colSize.at(c)+1)
                << std::right
                << l.at(c) << ' ';
    }
    std::cout << std::endl;
  }

  // Empty buffer
  m_outputBuffer.clear();
}


void TextFormatter::printAfLsHeader() {
  push_back(
    "archive id",
    "copy no",
    "vid",
    "fseq",
    "block id",
    "instance",
    "disk id",
    "size",
    "checksum type",
    "checksum value",
    "storage class",
    "owner",
    "group",
    "creation time",
    "ss vid",
    "ss fseq",
    "path"
  );
}

void TextFormatter::print(const cta::admin::ArchiveFileLsItem &afls_item)
{
  push_back(
    afls_item.af().archive_id(),
    afls_item.copy_nb(),
    afls_item.tf().vid(),
    afls_item.tf().f_seq(),
    afls_item.tf().block_id(),
    afls_item.af().disk_instance(),
    afls_item.af().disk_id(),
    afls_item.af().size(),
    afls_item.af().cs().type(),
    afls_item.af().cs().value(),
    afls_item.af().storage_class(),
    afls_item.af().df().owner(),
    afls_item.af().df().group(),
    afls_item.af().creation_time(),
    afls_item.tf().superseded_by_vid().size() ? afls_item.tf().superseded_by_vid() : "-",
    afls_item.tf().superseded_by_vid().size() ? std::to_string(afls_item.tf().superseded_by_f_seq()) : "-",
    afls_item.af().df().path()
  );
}

void TextFormatter::printAfLsSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::ArchiveFileLsSummary &afls_summary)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << afls_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << afls_summary.total_size()  << ' '
             << std::endl;
}

void TextFormatter::printFrLsHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(12) << std::right << "request type"   << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "copy no"        << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "tapepool/vid"   << ' '
             << std::setfill(' ') << std::setw(10) << std::right << "requester"      << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << "group"          << ' '
                                                                 << "path"
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::FailedRequestLsItem &frls_item)
{
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

   std::cout << std::setfill(' ') << std::setw(11) << std::right << request_type                      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << frls_item.copy_nb()               << ' '
             << std::setfill(' ') << std::setw(14) << std::right << tapepool_vid                      << ' '
             << std::setfill(' ') << std::setw(10) << std::right << frls_item.requester().username()  << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << frls_item.requester().groupname() << ' '
                                                                 << frls_item.af().df().path()
             << std::endl;

   for(auto &errLogMsg : frls_item.failurelogs()) {
     std::cout << errLogMsg << std::endl;
   }
}

void TextFormatter::printFrLsSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(12) << std::right << "request type"        << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files"         << ' '
             << std::setfill(' ') << std::setw(20) << std::right << "total size (bytes)"  << ' '
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::FailedRequestLsSummary &frls_summary)
{
   std::string request_type =
      frls_summary.request_type() == cta::admin::RequestType::ARCHIVE_REQUEST  ? "archive" :
      frls_summary.request_type() == cta::admin::RequestType::RETRIEVE_REQUEST ? "retrieve" : "total";

   std::cout << std::setfill(' ') << std::setw(11) << std::right << request_type               << ' '
             << std::setfill(' ') << std::setw(13) << std::right << frls_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(20) << std::right << frls_summary.total_size()  << ' '
             << std::endl;
}

void TextFormatter::printLpaHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "tapepool"       << ' '
             << std::setfill(' ') << std::setw(11) << std::right << "archive id"     << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "storage class"  << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "disk id"        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "instance"       << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "checksum type"  << ' '
             << std::setfill(' ') << std::setw(14) << std::right << "checksum value" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "size"           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "user"           << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' '
             <<                                                     "path"
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::ListPendingArchivesItem &lpa_item)
{
   std::cout << std::setfill(' ') << std::setw(18) << std::right << lpa_item.tapepool()           << ' '
             << std::setfill(' ') << std::setw(11) << std::right << lpa_item.af().archive_id()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_item.af().storage_class() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpa_item.copy_nb()            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpa_item.af().disk_id()       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().disk_instance() << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_item.af().cs().type()     << ' '
             << std::setfill(' ') << std::setw(14) << std::right << lpa_item.af().cs().value()    << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpa_item.af().size()          << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().df().owner()    << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpa_item.af().df().group()    << ' '
             <<                                                     lpa_item.af().df().path()
             << std::endl;
}

void TextFormatter::printLpaSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "tapepool"    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::ListPendingArchivesSummary &lpa_summary)
{
   std::cout << std::setfill(' ') << std::setw(18) << std::right << lpa_summary.tapepool()    << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpa_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpa_summary.total_size()  << ' '
             << std::endl;
}

void TextFormatter::printLprHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "vid"        << ' '
             << std::setfill(' ') << std::setw(11) << std::right << "archive id" << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "copy no"    << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "fseq"       << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << "block id"   << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "size"       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "user"       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "group"      << ' '
             <<                                                     "path"
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::ListPendingRetrievesItem &lpr_item)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << lpr_item.tf().vid()        << ' '
             << std::setfill(' ') << std::setw(11) << std::right << lpr_item.af().archive_id() << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpr_item.copy_nb()         << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << lpr_item.tf().f_seq()      << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << lpr_item.tf().block_id()   << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpr_item.af().size()       << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpr_item.af().df().owner() << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << lpr_item.af().df().group() << ' '
             <<                                                     lpr_item.af().df().path()
             << std::endl;
}

void TextFormatter::printLprSummaryHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(13) << std::right << "vid"         << ' '
             << std::setfill(' ') << std::setw(13) << std::right << "total files" << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "total size"  << ' '
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::ListPendingRetrievesSummary &lpr_summary)
{
   std::cout << std::setfill(' ') << std::setw(13) << std::right << lpr_summary.vid()         << ' '
             << std::setfill(' ') << std::setw(13) << std::right << lpr_summary.total_files() << ' '
             << std::setfill(' ') << std::setw(12) << std::right << lpr_summary.total_size()  << ' '
             << std::endl;
}

void TextFormatter::printTpLsHeader()
{
   std::cout << TEXT_RED
             << std::setfill(' ') << std::setw(18) << std::right << "name"        << ' '
             << std::setfill(' ') << std::setw(10) << std::right << "vo"          << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << "#tapes"      << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << "#partial"    << ' '
             << std::setfill(' ') << std::setw(12) << std::right << "#phys files" << ' '
             << std::setfill(' ') << std::setw(5)  << std::right << "size"        << ' '
             << std::setfill(' ') << std::setw(5)  << std::right << "used"        << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << "avail"       << ' '
             << std::setfill(' ') << std::setw(6)  << std::right << "use%"        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "encrypt"     << ' '
             << std::setfill(' ') << std::setw(20) << std::right << "supply"      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "c.user"      << ' '
             << std::setfill(' ') << std::setw(25) << std::right << "c.host"      << ' '
             << std::setfill(' ') << std::setw(24) << std::right << "c.time"      << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << "m.user"      << ' '
             << std::setfill(' ') << std::setw(25) << std::right << "m.host"      << ' '
             << std::setfill(' ') << std::setw(24) << std::right << "m.time"      << ' '
             <<                                                     "comment"     << ' '
             << TEXT_NORMAL << std::endl;
}

void TextFormatter::printTapeLsHeader(){
  std::cout << TEXT_RED
            << std::setfill(' ') << std::setw(7) << std::right << "vid"              << ' '
            << std::setfill(' ') << std::setw(10) << std::right << "media type"       << ' '
            << std::setfill(' ') << std::setw(7)  << std::right << "vendor"           << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << "logical library"  << ' '
            << std::setfill(' ') << std::setw(18) << std::right << "tapepool"         << ' '
            << std::setfill(' ') << std::setw(10)  << std::right << "vo"               << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << "encryption key"   << ' '
            << std::setfill(' ') << std::setw(12)  << std::right << "capacity"         << ' '
            << std::setfill(' ') << std::setw(12)  << std::right << "occupancy"        << ' '
            << std::setfill(' ') << std::setw(9)  << std::right << "last fseq"        << ' '
            << std::setfill(' ') << std::setw(5)  << std::right << "full"             << ' '
            << std::setfill(' ') << std::setw(8) << std::right << "disabled"         << ' '
            << std::setfill(' ') << std::setw(12) << std::right << "label drive"      << ' '
            << std::setfill(' ') << std::setw(12)  << std::right << "label time"       << ' '
            << std::setfill(' ') << std::setw(12) << std::right << "last w drive"     << ' '
            << std::setfill(' ') << std::setw(12) << std::right << "last w time"      << ' '
            << std::setfill(' ') << std::setw(12) << std::right << "last r drive"     << ' '
            << std::setfill(' ') << std::setw(12) << std::right << "last r time"      << ' '
            << std::setfill(' ') << std::setw(20) << std::right << "c.user"           << ' '
            << std::setfill(' ') << std::setw(25) << std::right << "c.host"           << ' '
            << std::setfill(' ') << std::setw(13) << std::right << "c.time"           << ' '
            << std::setfill(' ') << std::setw(20) << std::right << "m.user"           << ' '
            << std::setfill(' ') << std::setw(25) << std::right << "m.host"           << ' '
            << std::setfill(' ') << std::setw(13) << std::right << "m.time"           << ' '
            <<                                                     "comment"          << ' '
            << TEXT_NORMAL << std::endl;
}


void TextFormatter::print(const cta::admin::TapeLsItem &tals_item){
  std::cout << std::setfill(' ') << std::setw(7) << std::right << tals_item.vid()        << ' '
            << std::setfill(' ') << std::setw(10) << std::right << tals_item.media_type() << ' '
            << std::setfill(' ') << std::setw(7)  << std::right << tals_item.vendor()     << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << tals_item.logical_library() << ' '
            << std::setfill(' ') << std::setw(18) << std::right << tals_item.tapepool()         << ' '
            << std::setfill(' ') << std::setw(10)  << std::right << tals_item.vo()               << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << tals_item.encryption_key()   << ' '
            << std::setfill(' ') << std::setw(12)  << std::right << tals_item.capacity()         << ' '
            << std::setfill(' ') << std::setw(12)  << std::right << tals_item.occupancy()       << ' '
            << std::setfill(' ') << std::setw(9)  << std::right << tals_item.last_fseq()       << ' '
            << std::setfill(' ') << std::setw(5)  << std::right << tals_item.full()           << ' '
            << std::setfill(' ') << std::setw(8) << std::right << tals_item.disabled()         << ' ';
  if(tals_item.has_label_log()){
    std::cout << std::setfill(' ') << std::setw(12) << std::right << tals_item.label_log().drive() << ' '
              << std::setfill(' ') << std::setw(12) << std::right << tals_item.label_log().time() << ' ';
  } else {
    std::cout << std::setfill(' ') << std::setw(12) << std::right << "-" << ' '
              << std::setfill(' ') << std::setw(12) << std::right << "-" << ' ';
  }
  if(tals_item.has_last_written_log()){
    std::cout << std::setfill(' ') << std::setw(12) << std::right << tals_item.last_written_log().drive() << ' '
              << std::setfill(' ') << std::setw(12) << std::right << tals_item.last_written_log().time() << ' ';
  } else {
    std::cout << std::setfill(' ') << std::setw(12) << "-" << ' '
              << std::setfill(' ') << std::setw(12) << "-" << ' ';
  }
  if(tals_item.has_last_read_log()){
    std::cout << std::setfill(' ') << std::setw(12) << std::right << tals_item.last_read_log().drive() << ' '
              << std::setfill(' ') << std::setw(12) << std::right << tals_item.last_read_log().time() << ' ';
  } else {
    std::cout << std::setfill(' ') << std::setw(12) << std::right << "-" << ' '
              << std::setfill(' ') << std::setw(12) << std::right << "-" << ' ';
  }
    std::cout << std::setfill(' ') << std::setw(20) << std::right << tals_item.creation_log().username()          << ' '
              << std::setfill(' ') << std::setw(25) << std::right << tals_item.creation_log().host()          << ' '
              << std::setfill(' ') << std::setw(13) << std::right << tals_item.creation_log().time()           << ' '
              << std::setfill(' ') << std::setw(20) << std::right << tals_item.last_modification_log().username()        << ' '
              << std::setfill(' ') << std::setw(25) << std::right << tals_item.last_modification_log().host()          << ' '
              << std::setfill(' ') << std::setw(13) << std::right << tals_item.last_modification_log().time()           << ' '
              << std::endl;
}

void TextFormatter::printRepackLsHeader(){
  std::cout << TEXT_RED
            << std::setfill(' ') << std::setw(7) << std::right << "vid"              << ' '
            << std::setfill(' ') << std::setw(50) << std::right << "repackBufferURL"       << ' '
            << std::setfill(' ') << std::setw(17)  << std::right << "userProvidedFiles"           << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << "totalFilesToRetrieve" << ' '
            << std::setfill(' ') << std::setw(19) << std::right <<  "totalBytesToRetrieve" << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << "totalFilesToArchive"  << ' '
            << std::setfill(' ') << std::setw(19) << std::right <<  "totalBytesToArchive"  << ' '
            << std::setfill(' ') << std::setw(14)  << std::right << "retrievedFiles"               << ' '
            << std::setfill(' ') << std::setw(13)  << std::right << "archivedFiles"   << ' '
            << std::setfill(' ') << std::setw(21)  << std::right << "failedToRetrieveFiles"         << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << "failedToRetrieveBytes"        << ' '
            << std::setfill(' ') << std::setw(20)  << std::right <<  "failedToArchiveFiles"        << ' '
            << std::setfill(' ') << std::setw(20)  << std::right <<  "failedToArchiveBytes"             << ' '
            << std::setfill(' ') << std::setw(16)  << std::right <<  "lastExpandedFSeq"             << ' '
            <<                                                      "status"         << ' '
            << TEXT_NORMAL << std::endl;
}

void TextFormatter::print(const cta::admin::RepackLsItem &rels_item){
  std::cout << std::setfill(' ') << std::setw(7) << std::right << rels_item.vid()           << ' '
            << std::setfill(' ') << std::setw(50) << std::right << rels_item.repack_buffer_url()      << ' '
            << std::setfill(' ') << std::setw(17)  << std::right << rels_item.user_provided_files()           << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << rels_item.total_files_to_retrieve() << ' '
            << std::setfill(' ') << std::setw(19) << std::right <<  rels_item.total_bytes_to_retrieve() << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << rels_item.total_files_to_archive()  << ' '
            << std::setfill(' ') << std::setw(19) << std::right <<  rels_item.total_bytes_to_archive()  << ' '
            << std::setfill(' ') << std::setw(14)  << std::right << rels_item.retrieved_files()               << ' '
            << std::setfill(' ') << std::setw(13)  << std::right << rels_item.archived_files()   << ' '
            << std::setfill(' ') << std::setw(21)  << std::right << rels_item.failed_to_retrieve_files()        << ' '
            << std::setfill(' ') << std::setw(20)  << std::right << rels_item.failed_to_retrieve_bytes()        << ' '
            << std::setfill(' ') << std::setw(20)  << std::right <<  rels_item.failed_to_archive_files()       << ' '
            << std::setfill(' ') << std::setw(20)  << std::right <<  rels_item.failed_to_retrieve_bytes()         << ' '
            << std::setfill(' ') << std::setw(10)  << std::right <<  rels_item.last_expanded_fseq()             << ' '
            << rels_item.status() << std::endl;
}

void TextFormatter::print(const cta::admin::TapePoolLsItem &tpls_item)
{
   std::string encrypt_str = tpls_item.encrypt() ? "true" : "false";
   uint64_t avail = tpls_item.capacity_bytes() > tpls_item.data_bytes() ?
      tpls_item.capacity_bytes()-tpls_item.data_bytes() : 0; 
   double use_percent = tpls_item.capacity_bytes() > 0 ?
      (static_cast<double>(tpls_item.data_bytes())/static_cast<double>(tpls_item.capacity_bytes()))*100.0 : 0.0;

   std::cout << std::setfill(' ') << std::setw(18) << std::right << tpls_item.name()                          << ' '
             << std::setfill(' ') << std::setw(10) << std::right << tpls_item.vo()                            << ' '
             << std::setfill(' ') << std::setw(7)  << std::right << tpls_item.num_tapes()                     << ' '
             << std::setfill(' ') << std::setw(9)  << std::right << tpls_item.num_partial_tapes()             << ' '
             << std::setfill(' ') << std::setw(12) << std::right << tpls_item.num_physical_files()            << ' '
             << std::setfill(' ') << std::setw(4)  << std::right << tpls_item.capacity_bytes() / 1000000000   << "G "
             << std::setfill(' ') << std::setw(4)  << std::right << tpls_item.data_bytes()     / 1000000000   << "G "
             << std::setfill(' ') << std::setw(5)  << std::right << avail                      / 1000000000   << "G "
             << std::setfill(' ') << std::setw(5)  << std::right << std::fixed << std::setprecision(1) << use_percent << "% "
             << std::setfill(' ') << std::setw(8)  << std::right << encrypt_str                               << ' '
             << std::setfill(' ') << std::setw(20) << std::right << tpls_item.supply()                        << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << tpls_item.created().username()            << ' '
             << std::setfill(' ') << std::setw(25) << std::right << tpls_item.created().host()                << ' '
             << std::setfill(' ') << std::setw(24) << std::right << timeToString(tpls_item.created().time())  << ' '
             << std::setfill(' ') << std::setw(8)  << std::right << tpls_item.modified().username()           << ' '
             << std::setfill(' ') << std::setw(25) << std::right << tpls_item.modified().host()               << ' '
             << std::setfill(' ') << std::setw(24) << std::right << timeToString(tpls_item.modified().time()) << ' '
             <<                                                     tpls_item.comment()
             << std::endl;
}

}}
