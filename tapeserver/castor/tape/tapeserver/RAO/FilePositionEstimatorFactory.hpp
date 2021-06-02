/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "FilePositionEstimator.hpp"
#include "common/log/TimingList.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * This class gathen static factory method to instanciate FilePositionEstimator
 */
class FilePositionEstimatorFactory {
public:
  static std::unique_ptr<FilePositionEstimator> createInterpolationFilePositionEstimator(const std::string & vid, cta::catalogue::Catalogue *catalogue, drive::DriveInterface *drive, cta::log::TimingList &tl);
  static std::unique_ptr<FilePositionEstimator> createInterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & eowp, cta::catalogue::MediaType & mediaType);
};

}}}}
