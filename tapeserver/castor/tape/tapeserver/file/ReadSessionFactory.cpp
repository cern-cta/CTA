/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>
#include <sstream>

#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeReadSession.hpp"
#include "castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape::tapeFile {

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
