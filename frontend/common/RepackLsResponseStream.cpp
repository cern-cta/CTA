/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include <set>
#include <algorithm>

#include "RepackLsResponseStream.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include "common/dataStructures/RepackInfo.hpp"

namespace cta::frontend {

RepackLsResponseStream::RepackLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                               cta::Scheduler& scheduler,
                                               const std::string& instanceName,
                                               const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(adminCmd);

  // Get optional VID parameter
  bool has_any = false;
  auto vid = request.getOptional(OptionString::VID, &has_any);

  collectRepacks(vid);
}

void RepackLsResponseStream::collectRepacks(const std::optional<std::string>& vid) {
  std::list<common::dataStructures::RepackInfo> repackList;

  if (!vid) {
    repackList = m_scheduler.getRepacks();
    repackList.sort(
      [](const common::dataStructures::RepackInfo& repackInfo1, const common::dataStructures::RepackInfo& repackInfo2) {
        return repackInfo1.creationLog.time < repackInfo2.creationLog.time;
      });
  } else {
    repackList.push_back(m_scheduler.getRepack(vid.value()));
  }

  if (repackList.empty()) {
    return;
  }

  // Collect all VIDs and get tape information
  std::set<std::string, std::less<>> tapeVids;
  std::transform(repackList.begin(),
                 repackList.end(),
                 std::inserter(tapeVids, tapeVids.begin()),
                 [](const common::dataStructures::RepackInfo& ri) { return ri.vid; });

  common::dataStructures::VidToTapeMap tapeVidMap = m_catalogue.Tape()->getTapesByVid(tapeVids);

  // Convert each repack to data items
  for (const auto& repackRequest : repackList) {
    cta::xrd::Data data;
    auto repackRequestItem = data.mutable_rels_item();

    uint64_t filesLeftToRetrieve = repackRequest.totalFilesToRetrieve - repackRequest.retrievedFiles;
    uint64_t filesLeftToArchive = repackRequest.totalFilesToArchive - repackRequest.archivedFiles;
    uint64_t totalFilesToRetrieve = repackRequest.totalFilesToRetrieve;
    uint64_t totalFilesToArchive = repackRequest.totalFilesToArchive;

    repackRequestItem->set_instance_name(m_instanceName);
    repackRequestItem->set_vid(repackRequest.vid);
    repackRequestItem->set_tapepool(tapeVidMap[repackRequest.vid].tapePoolName);
    repackRequestItem->set_repack_buffer_url(repackRequest.repackBufferBaseURL);
    repackRequestItem->set_user_provided_files(repackRequest.userProvidedFiles);
    repackRequestItem->set_total_files_on_tape_at_start(repackRequest.totalFilesOnTapeAtStart);
    repackRequestItem->set_total_bytes_on_tape_at_start(repackRequest.totalBytesOnTapeAtStart);
    repackRequestItem->set_all_files_selected_at_start(repackRequest.allFilesSelectedAtStart);
    repackRequestItem->set_total_files_to_retrieve(totalFilesToRetrieve);
    repackRequestItem->set_total_bytes_to_retrieve(repackRequest.totalBytesToRetrieve);
    repackRequestItem->set_total_files_to_archive(totalFilesToArchive);
    repackRequestItem->set_total_bytes_to_archive(repackRequest.totalBytesToArchive);
    repackRequestItem->set_retrieved_files(repackRequest.retrievedFiles);
    repackRequestItem->set_retrieved_bytes(repackRequest.retrievedBytes);
    repackRequestItem->set_files_left_to_retrieve(filesLeftToRetrieve);
    repackRequestItem->set_files_left_to_archive(filesLeftToArchive);
    repackRequestItem->set_archived_files(repackRequest.archivedFiles);
    repackRequestItem->set_archived_bytes(repackRequest.archivedBytes);
    repackRequestItem->set_failed_to_retrieve_files(repackRequest.failedFilesToRetrieve);
    repackRequestItem->set_failed_to_retrieve_bytes(repackRequest.failedBytesToRetrieve);
    repackRequestItem->set_failed_to_archive_files(repackRequest.failedFilesToArchive);
    repackRequestItem->set_failed_to_archive_bytes(repackRequest.failedBytesToArchive);
    repackRequestItem->set_total_failed_files(repackRequest.failedFilesToRetrieve + repackRequest.failedFilesToArchive);
    repackRequestItem->set_status(toString(repackRequest.status));
    uint64_t repackTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - repackRequest.creationLog.time;
    repackRequestItem->set_repack_finished_time(repackRequest.repackFinishedTime);
    if (repackRequest.repackFinishedTime != 0) {
      //repackFinishedTime != 0: repack is finished
      repackTime = repackRequest.repackFinishedTime - repackRequest.creationLog.time;
    }
    repackRequestItem->set_repack_time(repackTime);
    repackRequestItem->mutable_creation_log()->set_username(repackRequest.creationLog.username);
    repackRequestItem->mutable_creation_log()->set_host(repackRequest.creationLog.host);
    repackRequestItem->mutable_creation_log()->set_time(repackRequest.creationLog.time);
    //Last expanded fSeq is in reality the next FSeq to Expand. So last one is next - 1
    repackRequestItem->set_last_expanded_fseq(repackRequest.lastExpandedFseq != 0 ? repackRequest.lastExpandedFseq - 1 :
                                                                                    0);
    repackRequestItem->mutable_destination_infos()->Clear();
    for (auto destinationInfo : repackRequest.destinationInfos) {
      auto* destinationInfoToInsert = repackRequestItem->mutable_destination_infos()->Add();
      destinationInfoToInsert->set_vid(destinationInfo.vid);
      destinationInfoToInsert->set_files(destinationInfo.files);
      destinationInfoToInsert->set_bytes(destinationInfo.bytes);
    }

    m_repackItems.emplace_back(std::move(data));
  }
}

bool RepackLsResponseStream::isDone() {
  return m_repackItems.empty();
}

cta::xrd::Data RepackLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  cta::xrd::Data data = std::move(m_repackItems.front());
  m_repackItems.pop_front();

  return data;
}

}  // namespace cta::frontend
