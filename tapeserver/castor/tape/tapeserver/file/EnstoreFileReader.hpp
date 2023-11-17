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

class EnstoreFileReader : public FileReader {
public:
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  /**
    * Constructor of the EnstoreFileReader. It will bind itself to an existing read session
    * and position the tape right at the beginning of the file
    * @param rs: session to be bound to
    * @param fileToRecall: the file which will be recalled
    */
  EnstoreFileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall);

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
