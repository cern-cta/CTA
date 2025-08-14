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

#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession1.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession2.hpp"
#include "castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeReadSession.hpp"
#include "castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape::tapeFile {

std::unique_ptr<ReadSession> ReadSessionFactory::create(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp, cta::utils::ReadTapeTestMode testMode) {
  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = volInfo.labelFormat;
  switch (labelFormat) {
    case LabelFormat::CTA:
      switch (testMode) {
        case cta::utils::ReadTapeTestMode::USE_FSEC:
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_DEFAULT: {
          return std::make_unique<CtaReadSession>(drive, volInfo, useLbp);
        }
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_1: {
          return std::make_unique<CtaReadSession1>(drive, volInfo, useLbp);
        }
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_2: {
          return std::make_unique<CtaReadSession2>(drive, volInfo, useLbp);
        }
        default: {
          throw TapeFormatError("In FileReaderFactory::create(): unknown test mode");
        }
      }
    case LabelFormat::OSM:
      return std::make_unique<OsmReadSession>(drive, volInfo, useLbp);
    case LabelFormat::Enstore:
      return std::make_unique<EnstoreReadSession>(drive, volInfo, useLbp);
    case LabelFormat::EnstoreLarge:
      return std::make_unique<EnstoreLargeReadSession>(drive, volInfo, useLbp);
    default: {
      std::ostringstream ossLabelFormat;
      ossLabelFormat << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                     << static_cast<unsigned int>(labelFormat);
      throw TapeFormatError("In ReadSessionFactory::create(): unknown label format: " + ossLabelFormat.str());
    }
  }
}

} // namespace castor::tape::tapeFile
