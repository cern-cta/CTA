/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
