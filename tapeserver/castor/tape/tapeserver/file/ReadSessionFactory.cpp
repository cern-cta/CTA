/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"

#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession1.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession2.hpp"
#include "castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "common/dataStructures/LabelFormat.hpp"

#include <memory>
#include <sstream>

namespace castor::tape::tapeFile {

std::unique_ptr<ReadSession> ReadSessionFactory::create(tapeserver::drive::DriveInterface& drive,
                                                        const tapeserver::daemon::VolumeInfo& volInfo,
                                                        const bool useLbp, cta::utils::ReadTapeTestMode testMode) {
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

}  // namespace castor::tape::tapeFile
