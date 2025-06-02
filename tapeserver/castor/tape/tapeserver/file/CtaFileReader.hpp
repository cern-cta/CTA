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

#include <memory>

#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"

namespace castor::tape::tapeFile {

class UHL1;

class CtaFileReader : public FileReader {
  using FileReader::FileReader;

public:
  /**
    * Destructor. It will release the lock on the read session.
    */
  ~CtaFileReader() override = default;

  size_t readNextDataBlock(void* data, const size_t size) override;

private:
  /**
    * Set the block size member using the info contained within the uhl1
    * @param uhl1: the uhl1 header of the current file
    * @param fileInfo: the Information structure of the current file
    */
  void setBlockSize(const UHL1& uhl1);

  void positionByFseq(const cta::RetrieveJob& fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob& fileToRecall) override;

  void moveToFirstHeaderBlock();
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void useBlockID(const cta::RetrieveJob& fileToRecall);
  void checkHeaders(const cta::RetrieveJob& fileToRecall);
  void checkTrailers();
};

}  // namespace castor::tape::tapeFile
