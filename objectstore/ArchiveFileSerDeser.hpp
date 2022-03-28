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
#include "TapeFileSerDeser.hpp"
#include "DiskFileInfoSerDeser.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta { namespace objectstore {
/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class ArchiveFileSerDeser: public cta::common::dataStructures::ArchiveFile {
public:
  ArchiveFileSerDeser (): cta::common::dataStructures::ArchiveFile() {}
  ArchiveFileSerDeser (const cta::common::dataStructures::ArchiveFile & af): cta::common::dataStructures::ArchiveFile(af) {}
  operator cta::common::dataStructures::ArchiveFile() {
    return cta::common::dataStructures::ArchiveFile(*this);
  } 
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
  
}}
