/**
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "objectstore/cta.pb.h"
#include "common/dataStructures/DiskFileInfo.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta { namespace objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
struct DiskFileInfoSerDeser: public cta::common::dataStructures::DiskFileInfo {
  DiskFileInfoSerDeser() : cta::common::dataStructures::DiskFileInfo() {}
  DiskFileInfoSerDeser(const cta::common::dataStructures::DiskFileInfo &dfi) : cta::common::dataStructures::DiskFileInfo(dfi) {}

  operator cta::common::dataStructures::DiskFileInfo() {
    return cta::common::dataStructures::DiskFileInfo(*this);
  }

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

}}
