/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "castor/tape/tapeserver/file/CpioFileHeaderStructure.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "tapeserver/castor/tape/tapeserver/file/OsmFileStructure.hpp"

#include <memory>

namespace castor::tape::tapeFile {

class OsmFileReader : public FileReader {
  using FileReader::FileReader;

public:
  /**
    * Destructor. It will release the lock on the read session.
    */
  ~OsmFileReader() override = default;

  size_t readNextDataBlock(void* data, const size_t size) override;

  const CPIO& getCPIOHeader() const { return m_cpioHeader; }

private:
  /*
   * The maximum payload size should be 262144,
   * but due to the mutated OSM tape format
   * it may happen that CRC32C is included twice e.g.:
   * Last 16 bytes of data block:
   *
   *  00 3c 00 38 00 43 00 39 93 3c 5d 26 c7 4b 67 48 
   *  -----------------------|-----------|
   *     DATA BLOCK              CRC32C
   *  -----------------------------------|-----------|
   *     DATA BLOCK                          CRC32C
   *
   * In such case, the payload size should be 262148.
   */
  const size_t PAYLOAD_BOLCK_SIZE = 262148;
  /*
   * Set to true if 64k_FORMAT detected
   */
  bool m_bDataWithCRC32 = false;
  /*
   * CPIO file
   */
  CPIO m_cpioHeader;
  uint64_t m_ui64CPIODataSize = 0;

  void positionByFseq(const cta::RetrieveJob& fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob& fileToRecall) override;

  void moveToFirstFile();
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void useBlockID(const cta::RetrieveJob& fileToRecall);
  void setBlockSize(size_t uiBlockSize);
};

}  // namespace castor::tape::tapeFile
