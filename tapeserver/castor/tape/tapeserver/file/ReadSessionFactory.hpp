/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape {

namespace tapeserver::drive {
class DriveInterface;
}

namespace tapeFile {

class ReadSession;

class ReadSessionFactory {
public:
  static std::unique_ptr<ReadSession> create(tapeserver::drive::DriveInterface &drive, const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp);
};

}} // namespace castor::tape::tapeFile
