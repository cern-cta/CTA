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

#include <fstream>
#include <memory>

#include "castor/tape/tapeserver/file/CpioFileHeaderStructure.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"

namespace castor::tape::tapeFile {

class UHL1;

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

private:
  void setPositioningMethod(const cta::PositioningMethod& newMethod);
  void positionByFseq(const cta::RetrieveJob& fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob& fileToRecall) override;
  void setBlockSize(const UHL1& uhl1);
  void moveToFirstHeaderBlock();  // Not implemented
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void checkHeaders(const cta::RetrieveJob& fileToRecall);
  void checkTrailers();
};

}  // namespace castor::tape::tapeFile
