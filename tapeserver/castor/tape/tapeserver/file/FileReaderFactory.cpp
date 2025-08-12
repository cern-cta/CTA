/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"

#include "castor/tape/tapeserver/file/CtaFileReader.hpp"
#include "castor/tape/tapeserver/file/CtaFileReader2.hpp"
#include "castor/tape/tapeserver/file/EnstoreFileReader.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeFileReader.hpp"
#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "common/dataStructures/LabelFormat.hpp"

#include <memory>
#include <sstream>

namespace castor::tape::tapeFile {

std::unique_ptr<FileReader> FileReaderFactory::create(ReadSession& readSession, const cta::RetrieveJob& fileToRecall, bool useAlternative) {
  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = readSession.getVolumeInfo().labelFormat;
  std::unique_ptr<FileReader> reader;
  switch (labelFormat) {
    case LabelFormat::CTA: {
      if (useAlternative) {
        reader = std::make_unique<CtaFileReader2>(readSession, fileToRecall);
      } else {
        reader = std::make_unique<CtaFileReader>(readSession, fileToRecall);
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

}  // namespace castor::tape::tapeFile
