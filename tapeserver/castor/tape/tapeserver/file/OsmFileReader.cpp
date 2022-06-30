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

#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

OsmFileReader::OsmFileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall)
  : FileReader(rs, fileToRecall) {
}

void OsmFileReader::positionByFseq(const cta::RetrieveJob &fileToRecall) {
  throw NotImplemented("OsmFileReader::positionByFseq() needs to be implemented");
}

void OsmFileReader::positionByBlockID(const cta::RetrieveJob &fileToRecall) {
  throw NotImplemented("OsmFileReader::positionByBlockID() needs to be implemented");
}

size_t OsmFileReader::readNextDataBlock(void *data, const size_t size) {
  throw NotImplemented("OsmFileReader::readNextDataBlock() needs to be implemented");
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
