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
