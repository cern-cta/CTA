/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "objectstore/cta.pb.h"
#include "common/dataStructures/TapeFile.hpp"
#include "EntryLogSerDeser.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class TapeFileSerDeser: public cta::common::dataStructures::TapeFile {
public:
  TapeFileSerDeser (): cta::common::dataStructures::TapeFile() {}
  explicit TapeFileSerDeser(const cta::common::dataStructures::TapeFile& tf) : cta::common::dataStructures::TapeFile(tf) {}

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
  
} // namespace cta::objectstore
