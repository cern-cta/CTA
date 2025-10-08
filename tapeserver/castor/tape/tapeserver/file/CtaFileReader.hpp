/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

  size_t readNextDataBlock(void *data, const size_t size) override;

private:
  /**
    * Set the block size member using the info contained within the uhl1
    * @param uhl1: the uhl1 header of the current file
    * @param fileInfo: the Information structure of the current file
    */
  void setBlockSize(const UHL1 &uhl1);

  void positionByFseq(const cta::RetrieveJob &fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob &fileToRecall) override;

  void moveToFirstHeaderBlock();
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void useBlockID(const cta::RetrieveJob &fileToRecall);
  void checkHeaders(const cta::RetrieveJob &fileToRecall);
  void checkTrailers();
};

} // namespace castor::tape::tapeFile
