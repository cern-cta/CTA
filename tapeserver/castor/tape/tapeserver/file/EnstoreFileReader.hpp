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

class EnstoreFileReader : public FileReader {
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
  EnstoreFileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall);

  /**
    * Destructor of the FileReader. It will release the lock on the read session.
    */
  ~EnstoreFileReader() override = default;

  size_t readNextDataBlock(void *data, const size_t size) override;

private:
  // Stuff for CPIO file
  CPIO m_cpioHeader;
  uint64_t m_ui64CPIODataSize = 0;

  void setPositioningMethod(const cta::PositioningMethod &newMethod);
  void positionByFseq(const cta::RetrieveJob &fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob &fileToRecall) override;
  void setBlockSize(const size_t uiBlockSize);
};

} // namespace castor::tape::tapeFile
