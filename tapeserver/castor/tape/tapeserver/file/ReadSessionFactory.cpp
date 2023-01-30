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

#include <memory>
#include <sstream>

#include "tapeserver/castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

std::unique_ptr<ReadSession> ReadSessionFactory::create(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp) {
  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = volInfo.labelFormat;
  switch (labelFormat) {
    case LabelFormat::CTA:
      return std::make_unique<CtaReadSession>(drive, volInfo, useLbp);
    case LabelFormat::OSM:
      return std::make_unique<OsmReadSession>(drive, volInfo, useLbp);
    case LabelFormat::Enstore:
      return std::make_unique<EnstoreReadSession>(drive, volInfo, useLbp);
    default: {
      std::ostringstream ossLabelFormat;
      ossLabelFormat << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                     << static_cast<unsigned int>(labelFormat);
      throw TapeFormatError("In ReadSessionFactory::create(): unknown label format: " + ossLabelFormat.str());
    }
  }
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
