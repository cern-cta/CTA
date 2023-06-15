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

#include <list>

#include "EnterpriseRAOAlgorithm.hpp"
#include "castor/tape/tapeserver/SCSI/Structures.hpp"
#include "common/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

EnterpriseRAOAlgorithm::EnterpriseRAOAlgorithm(castor::tape::tapeserver::drive::DriveInterface* drive,
                                               const uint64_t maxFilesSupported) :
m_drive(drive),
m_maxFilesSupported(maxFilesSupported) {}

EnterpriseRAOAlgorithm::~EnterpriseRAOAlgorithm() {}

std::vector<uint64_t> EnterpriseRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) {
  cta::utils::Timer totalTimer;
  std::vector<uint64_t> raoOrder;
  uint64_t njobs = jobs.size();
  uint32_t block_size = c_blockSize;
  std::list<castor::tape::SCSI::Structures::RAO::blockLims> files;
  for (uint32_t i = 0; i < njobs; i++) {
    cta::RetrieveJob* job = jobs.at(i).get();
    castor::tape::SCSI::Structures::RAO::blockLims lims;
    strncpy((char*) lims.fseq, std::to_string(i).c_str(), sizeof(i));
    lims.begin = job->selectedTapeFile().blockId;
    lims.end = job->selectedTapeFile().blockId + 8 +
               /* ceiling the number of blocks */
               ((job->archiveFile.fileSize + block_size - 1) / block_size);

    files.push_back(lims);
    if ((files.size() == m_maxFilesSupported) || ((i == njobs - 1) && (files.size() > 1))) {
      /* We do a RAO query if:
       *  1. the maximum number of files supported by the drive
       *     for RAO query has been reached
       *  2. the end of the jobs list has been reached and there are at least
       *     2 unordered files
       */
      m_drive->queryRAO(files, m_maxFilesSupported);

      /* Add the RAO sorted files to the new list*/
      for (auto fit = files.begin(); fit != files.end(); fit++) {
        uint64_t id = atoi((char*) fit->fseq);
        raoOrder.push_back(id);
      }
      files.clear();
    }
  }
  for (auto fit = files.begin(); fit != files.end(); fit++) {
    uint64_t id = atoi((char*) fit->fseq);
    raoOrder.push_back(id);
  }
  files.clear();
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return raoOrder;
}

std::string EnterpriseRAOAlgorithm::getName() const {
  return "enterprise";
}

}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor