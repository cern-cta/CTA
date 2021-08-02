/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "objectstore/cta.pb.h"
#include "common/dataStructures/TapeFile.hpp"
#include "EntryLogSerDeser.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta { namespace objectstore {
/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class TapeFileSerDeser: public cta::common::dataStructures::TapeFile {
public:
  TapeFileSerDeser (): cta::common::dataStructures::TapeFile() {}
  TapeFileSerDeser (const cta::common::dataStructures::TapeFile & tf): cta::common::dataStructures::TapeFile(tf) {}
  operator cta::common::dataStructures::TapeFile() {
    return cta::common::dataStructures::TapeFile(*this);
  } 
  void serialize (cta::objectstore::serializers::TapeFile & ostf) const {
    ostf.set_vid(vid);
    ostf.set_fseq(fSeq);
    ostf.set_blockid(blockId);
    ostf.set_filesize(fileSize);
    ostf.set_copynb(copyNb);
    ostf.set_creationtime(creationTime);
    ostf.set_checksumblob(checksumBlob.serialize());
  }
  
  void deserialize (const cta::objectstore::serializers::TapeFile & ostf) {
    vid=ostf.vid();
    fSeq=ostf.fseq();
    blockId=ostf.blockid();
    fileSize=ostf.filesize();
    copyNb=ostf.copynb();
    creationTime=ostf.creationtime();
    checksumBlob.deserialize(ostf.checksumblob());
  }
};
  
}}
