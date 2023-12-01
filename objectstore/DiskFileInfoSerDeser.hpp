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
#include "common/dataStructures/DiskFileInfo.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
struct DiskFileInfoSerDeser: public cta::common::dataStructures::DiskFileInfo {
  DiskFileInfoSerDeser() : cta::common::dataStructures::DiskFileInfo() {}
  explicit DiskFileInfoSerDeser(const cta::common::dataStructures::DiskFileInfo& dfi) : cta::common::dataStructures::DiskFileInfo(dfi) {}

  void serialize (cta::objectstore::serializers::DiskFileInfo & osdfi) const {
    osdfi.set_path(path);
    osdfi.set_owner_uid(owner_uid);
    osdfi.set_gid(gid);
  }

  void deserialize (const cta::objectstore::serializers::DiskFileInfo & osdfi) {
    path      = osdfi.path();
    owner_uid = osdfi.owner_uid();
    gid       = osdfi.gid();
  }
};

} // namespace cta::objectstore
