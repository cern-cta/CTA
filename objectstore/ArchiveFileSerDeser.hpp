/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "objectstore/cta.pb.h"
#include "common/dataStructures/TapeFile.hpp"
#include "EntryLogSerDeser.hpp"
#include "TapeFileSerDeser.hpp"
#include "DiskFileInfoSerDeser.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class ArchiveFileSerDeser: public cta::common::dataStructures::ArchiveFile {
public:
  ArchiveFileSerDeser() : cta::common::dataStructures::ArchiveFile() {}
  explicit ArchiveFileSerDeser(const cta::common::dataStructures::ArchiveFile& af) : cta::common::dataStructures::ArchiveFile(af) {}

  void serialize (cta::objectstore::serializers::ArchiveFile & osaf) const {
    osaf.set_archivefileid(archiveFileID);
    osaf.set_creationtime(creationTime);
    osaf.set_checksumblob(checksumBlob.serialize());
    osaf.set_creationtime(creationTime);
    DiskFileInfoSerDeser dfisd(diskFileInfo);
    dfisd.serialize(*osaf.mutable_diskfileinfo());
    osaf.set_diskfileid(diskFileId);
    osaf.set_diskinstance(diskInstance);
    osaf.set_filesize(fileSize);
    osaf.set_reconciliationtime(reconciliationTime);
    osaf.set_storageclass(storageClass);
    for (auto & tf: tapeFiles)
      TapeFileSerDeser(tf).serialize(*osaf.add_tapefiles());
  }

  void deserialize (const cta::objectstore::serializers::ArchiveFile & osaf) {
    tapeFiles.clear();
    archiveFileID=osaf.archivefileid();
    creationTime=osaf.creationtime();
    checksumBlob.deserialize(osaf.checksumblob());
    diskFileId=osaf.diskfileid();
    DiskFileInfoSerDeser dfisd;
    dfisd.deserialize(osaf.diskfileinfo());
    diskFileInfo=dfisd;
    diskInstance=osaf.diskinstance();
    // TODO rename to filesize.
    fileSize=osaf.filesize();
    reconciliationTime=osaf.reconciliationtime();
    storageClass=osaf.storageclass();
    for (auto tf: osaf.tapefiles()) {
      TapeFileSerDeser tfsd;
      tfsd.deserialize(tf);
      tapeFiles.push_back(tfsd);
    }
  }
};

} // namespace cta::objectstore
