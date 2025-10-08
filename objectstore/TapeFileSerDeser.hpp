/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
