/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "common/exception/Exception.hpp"

namespace castor::tape::tapeFile {

class TapeFormatError: public cta::exception::Exception {
public:
  explicit TapeFormatError(const std::string & what): Exception(what) {}
};

class TapeMediaError: public cta::exception::Exception {
public:
  explicit TapeMediaError(const std::string & what): Exception(what) {}
};

class EndOfFile: public cta::exception::Exception {
public:
  EndOfFile(): Exception("End Of File reached") {}
};

class SessionAlreadyInUse: public cta::exception::Exception {
public:
  SessionAlreadyInUse(): Exception("Session already in use") {}
};

class SessionCorrupted: public cta::exception::Exception {
public:
  SessionCorrupted(): Exception("Session corrupted") {}
};

class FileClosedTwice: public cta::exception::Exception {
public:
  FileClosedTwice(): Exception("Trying to close a file twice") {}
};

class ZeroFileWritten: public cta::exception::Exception {
public:
  ZeroFileWritten(): Exception("Trying to write a file with size 0") {}
};

class TapeNotEmpty: public cta::exception::Exception {
public:
  TapeNotEmpty(): Exception("Trying to label a non-empty tape without the \"force\" setting") {}
};

class UnsupportedPositioningMode: public cta::exception::Exception {
public:
  UnsupportedPositioningMode(): Exception("Trying to use an unsupported positioning mode") {}
};

class WrongBlockSize: public cta::exception::Exception {
public:
  WrongBlockSize(): Exception("Trying to use a wrong block size") {}
};

} // namespace castor::tape::tapeFile
