/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include "common/ReadTapeTestMode.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape {

namespace tapeserver::drive {
class DriveInterface;
}

namespace tapeFile {

class ReadSession;

class ReadSessionFactory {
public:
  static std::unique_ptr<ReadSession>
  create(tapeserver::drive::DriveInterface& drive, const tapeserver::daemon::VolumeInfo& volInfo, const bool useLbp, cta::utils::ReadTapeTestMode testMode=cta::utils::ReadTapeTestMode::USE_FSEC);
};

}  // namespace tapeFile
}  // namespace castor::tape
