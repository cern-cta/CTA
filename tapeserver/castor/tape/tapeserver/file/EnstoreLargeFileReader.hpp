/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <fstream>
#include <memory>

#include "castor/tape/tapeserver/file/CpioFileHeaderStructure.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"

namespace castor::tape::tapeFile {

class UHL1;
class UHL2;

class EnstoreLargeFileReader : public FileReader {
public:
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

  /**
   * Constructor
   *
   * It will bind itself to an existing read session and position the tape right at the beginning of the file
   *
   * @param rs           session to bind to
   * @param fileToRecall the file which will be recalled
   */
  EnstoreLargeFileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall);

  /**
    * Destructor of the FileReader. It will release the lock on the read session.
    */
  ~EnstoreLargeFileReader() override = default;

  size_t readNextDataBlock(void* data, const size_t size) override;

protected:
  uint64_t bytes_to_read;

private:
  void setPositioningMethod(const cta::PositioningMethod& newMethod);
  void positionByFseq(const cta::RetrieveJob& fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob& fileToRecall) override;
  void setBlockSize(const UHL1& uhl1);
  void setTargetFileSize(const UHL2& uhl2);
  void moveToFirstHeaderBlock();  // Not implemented
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void checkHeaders(const cta::RetrieveJob& fileToRecall);
  void checkTrailers();
};

}  // namespace castor::tape::tapeFile
