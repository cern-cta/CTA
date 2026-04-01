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


#include "castor/tape/tapeserver/file/CpioFileHeaderStructure.hpp"
#include "tapeserver/castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"

#include <memory>

namespace castor::tape::tapeFile {

class OsmFileReader : public FileReader {
  using FileReader::FileReader;

public:
  /**
    * Destructor. It will release the lock on the read session.
    */
  ~OsmFileReader() override = default;

  size_t readNextDataBlock(void *data, const size_t size) override;
  
  const CPIO& getCPIOHeader() const {
    return m_cpioHeader;
  }

private:
  /*
   * The maximum payload size should be 262144,
   * but due to the mutated OSM tape format
   * it may happen that CRC32C isincluded twice e.g.:
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

  
  void positionByFseq(const cta::RetrieveJob &fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob &fileToRecall) override;
  
  void moveToFirstFile();
  void moveReaderByFSeqDelta(const int64_t fSeq_delta);
  void useBlockID(const cta::RetrieveJob &fileToRecall);
  void setBlockSize(size_t uiBlockSize);
};

} // namespace castor::tape::tapeFile
