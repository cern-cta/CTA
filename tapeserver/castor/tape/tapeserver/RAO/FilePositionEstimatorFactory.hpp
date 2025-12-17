/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "common/log/TimingList.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/FilePositionEstimator.hpp"

#include <memory>
#include <string>
#include <vector>

namespace cta::catalogue {
class Catalogue;
}

namespace castor::tape::tapeserver::rao {

/**
 * This class gathen static factory method to instanciate FilePositionEstimator
 */
class FilePositionEstimatorFactory {
public:
  static std::unique_ptr<FilePositionEstimator>
  createInterpolationFilePositionEstimator(const std::string& vid,
                                           cta::catalogue::Catalogue* catalogue,
                                           drive::DriveInterface* drive,
                                           cta::log::TimingList& tl);
  static std::unique_ptr<FilePositionEstimator>
  createInterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition>& eowp,
                                           cta::catalogue::MediaType& mediaType);
};

}  // namespace castor::tape::tapeserver::rao
