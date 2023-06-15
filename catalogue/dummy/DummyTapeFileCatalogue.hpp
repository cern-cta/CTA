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

#include "catalogue/interfaces/TapeFileCatalogue.hpp"

namespace cta {
namespace catalogue {

class DummyTapeFileCatalogue : public TapeFileCatalogue {
public:
  DummyTapeFileCatalogue() = default;
  ~DummyTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override;

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile& file, const std::string& reason) override;

  common::dataStructures::RetrieveFileQueueCriteria
    prepareToRetrieveFile(const std::string& diskInstanceName,
                          const uint64_t archiveFileId,
                          const common::dataStructures::RequesterIdentity& user,
                          const std::optional<std::string>& activity,
                          log::LogContext& lc,
                          const std::optional<std::string>& mountPolicyName = std::nullopt) override;
};

}  // namespace catalogue
}  // namespace cta