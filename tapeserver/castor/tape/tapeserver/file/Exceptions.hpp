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

#pragma once

#include <string>

#include "common/exception/Exception.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

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

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
