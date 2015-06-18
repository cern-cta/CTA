/*
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

#include "common/UserIdentity.hpp"
#include "objectstore/cta.pb.h"

#include <string>
#include <stdint.h>

namespace cta { namespace objectstore {

class UserIdentity: public cta::UserIdentity {
public:
  UserIdentity (): cta::UserIdentity() {}
  UserIdentity (uint32_t ui, uint32_t gi): cta::UserIdentity(ui, gi) {}
  void serialize (cta::objectstore::serializers::UserIdentity & user) const {
    user.set_uid(uid);
    user.set_gid(gid);
  }
  void deserialize (const cta::objectstore::serializers::UserIdentity & user) {
    uid = user.uid();
    gid = user.gid();
  }
};

}}
