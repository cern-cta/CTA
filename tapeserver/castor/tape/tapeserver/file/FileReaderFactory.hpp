/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "common/dataStructures/LabelFormat.hpp"

namespace cta {
class RetrieveJob;
}

namespace castor::tape::tapeFile {

class FileReader;
class ReadSession;

class FileReaderFactory {
public:
  static std::unique_ptr<FileReader> create(ReadSession& readSession, const cta::RetrieveJob& fileToRecall);
};

} // namespace castor::tape::tapeFile
