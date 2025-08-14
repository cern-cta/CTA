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
#include "castor/tape/tapeserver/file/CtaFileReader1.hpp"
#include "castor/tape/tapeserver/file/CtaFileReader2.hpp"
#include "castor/tape/tapeserver/file/EnstoreFileReader.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeFileReader.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape::tapeFile {

std::unique_ptr<FileReader> FileReaderFactory::create(ReadSession& readSession, const cta::RetrieveJob& fileToRecall, cta::utils::ReadTapeTestMode testMode) {
  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = readSession.getVolumeInfo().labelFormat;
  std::unique_ptr<FileReader> reader;
  switch (labelFormat) {
    case LabelFormat::CTA: {
      switch (testMode) {
        case cta::utils::ReadTapeTestMode::USE_FSEC:
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_DEFAULT: {
          reader = std::make_unique<CtaFileReader>(readSession, fileToRecall);
          break;
        }
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_1: {
          reader = std::make_unique<CtaFileReader1>(readSession, fileToRecall);
          break;
        }
        case cta::utils::ReadTapeTestMode::USE_BLOCK_ID_2: {
          reader = std::make_unique<CtaFileReader2>(readSession, fileToRecall);
          break;
        }
        default: {
          throw TapeFormatError("In FileReaderFactory::create(): unknown test mode");
        }
      }
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
    case LabelFormat::EnstoreLarge: {
      reader = std::make_unique<EnstoreLargeFileReader>(readSession, fileToRecall);
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

} // namespace castor::tape::tapeFile
