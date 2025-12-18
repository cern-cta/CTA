/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"

namespace cta::catalogue {

class DummyFileRecycleLogCatalogue : public FileRecycleLogCatalogue {
public:
  DummyFileRecycleLogCatalogue() = default;
  ~DummyFileRecycleLogCatalogue() override = default;

  FileRecycleLogItor getFileRecycleLogItor(
    const RecycleTapeFileSearchCriteria& searchCriteria = RecycleTapeFileSearchCriteria()) const override;

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria& searchCriteria, const std::string& newFid) override;

  void deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) override;
};

}  // namespace cta::catalogue
