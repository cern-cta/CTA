/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <readtp/ReadtpCmd.hpp>

#include "common/dataStructures/LabelFormat.hpp"

#include <memory>

namespace cta {
class RetrieveJob;
}

namespace castor::tape::tapeFile {

class FileReader;
class ReadSession;

class FileReaderFactory {
public:
  static std::unique_ptr<FileReader> create(ReadSession& readSession, const cta::RetrieveJob& fileToRecall, cta::utils::ReadTapeTestMode testMode=cta::utils::ReadTapeTestMode::USE_FSEC);
};

}  // namespace castor::tape::tapeFile
