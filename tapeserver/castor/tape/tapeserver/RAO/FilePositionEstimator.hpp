/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "FilePositionInfos.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * This abstract class allows its implementations to return the position
 * of a file represented by a cta::RetrieveJob
 */
class FilePositionEstimator {
public:
  FilePositionEstimator() = default;
  virtual ~FilePositionEstimator() = default;

  /**
   * Returns the position of the file corresponding to the RetrieveJob passed in parameter.
   * @param job the file corresponding to this job for which the position should be returned
   * @return the position of this file
   */
  virtual FilePositionInfos getFilePosition(const cta::RetrieveJob& job) const = 0;
};

} // namespace castor::tape::tapeserver::rao
