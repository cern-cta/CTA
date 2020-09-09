/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "FilePositionInfos.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
/**
 * This abstract class allows its implementations to return the position
 * of a file represented by a cta::RetrieveJob
 */
class FilePositionEstimator {
public:
  FilePositionEstimator();
  /**
   * Returns the position of the file corresponding to the RetrieveJob passed in parameter.
   * @param job the file corresponding to this job for which the position should be returned
   * @return the position of this file
   */
  virtual FilePositionInfos getFilePosition(const cta::RetrieveJob & job) const = 0;
  virtual ~FilePositionEstimator();
private:

};

}}}}