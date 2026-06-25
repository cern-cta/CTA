/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/LabelFormat.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <memory>

namespace cta::tape {

namespace drive {
class DriveInterface;
}

namespace tapeFile {

class ReadSession;

class ReadSessionFactory {
public:
  static std::unique_ptr<ReadSession>
  create(drive::DriveInterface& drive, const daemon::VolumeInfo& volInfo, const bool useLbp);
};

}  // namespace tapeFile
}  // namespace cta::tape
