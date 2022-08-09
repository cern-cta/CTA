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

#include "castor/tape/tapeserver/file/CtaFileReader.hpp"
#include "castor/tape/tapeserver/file/EnstoreFileReader.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

std::unique_ptr<FileReader> FileReaderFactory::create(const std::unique_ptr<ReadSession> &readSession,
                                                      const cta::RetrieveJob &fileToRecall) {
  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = readSession->getVolumeInfo().labelFormat;
  std::unique_ptr<FileReader> reader;
  switch (labelFormat) {
    case LabelFormat::CTA: {
      reader = std::make_unique<CtaFileReader>(readSession, fileToRecall);
      break;
    }
    case LabelFormat::OSM: {
      reader = std::make_unique<OsmFileReader>(readSession, fileToRecall);
      break;
    }
    case LabelFormat::Enstore: {
      reader = std::make_unique<EnstoreFileReader>(readSession, fileToRecall);
      break;
    }
    default: {
      std::ostringstream ossLabelFormat;
      ossLabelFormat << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                     << static_cast<unsigned int>(labelFormat);
      throw TapeFormatError("In FileReaderFactory::create(): unknown label format: " + ossLabelFormat.str());
    }
  }
  reader->position(fileToRecall);
  return reader;
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
