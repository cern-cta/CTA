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

#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

class UHL1;

class OsmFileReader : public FileReader {
public:
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  /**
    * Constructor of the FileReader. It will bind itself to an existing read session
    * and position the tape right at the beginning of the file
    * @param rs: session to be bound to
    * @param fileInfo: information about the file we would like to read
    * @param positioningMode: method used when positioning (see the PositioningMode enum)
    */
  OsmFileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall);

  /**
    * Destructor of the FileReader. It will release the lock on the read session.
    */
  ~OsmFileReader() override = default;

  size_t readNextDataBlock(void *data, const size_t size) override;

private:
  void positionByFseq(const cta::RetrieveJob &fileToRecall) override;
  void positionByBlockID(const cta::RetrieveJob &fileToRecall) override;
};

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
