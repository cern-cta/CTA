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

#include <memory>
#include <string>

#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "catalogue/RecyleTapeFileSearchCriteria.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class FileRecycleLogCatalogueRetryWrapper : public FileRecycleLogCatalogue {
public:
  FileRecycleLogCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                      log::Logger& m_log,
                                      const uint32_t maxTriesToConnect);

  ~FileRecycleLogCatalogueRetryWrapper() override = default;

  FileRecycleLogItor getFileRecycleLogItor(
    const RecycleTapeFileSearchCriteria& searchCriteria = RecycleTapeFileSearchCriteria()) const override;

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria& searchCriteria, const std::string& newFid) override;

  void deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};

}  // namespace catalogue
}  // namespace cta
