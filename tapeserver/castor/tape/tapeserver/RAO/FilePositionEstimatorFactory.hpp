/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
