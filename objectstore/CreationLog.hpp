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

#include <string>
#include <stdint.h>
#include <limits>
#include "scheduler/CreationLog.hpp"
#include "scheduler/UserIdentity.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {
/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class CreationLog: public cta::CreationLog {
public:
  CreationLog (): cta::CreationLog() {}
  CreationLog (const cta::UserIdentity& user, const std::string & hn, uint64_t t,
    const std::string & c): cta::CreationLog(
      cta::UserIdentity(user),hn ,t, c) {}
  void serialize (cta::objectstore::serializers::CreationLog & log) const {
    log.mutable_user()->set_uid(user.uid);
    log.mutable_user()->set_gid(user.gid);
    log.set_host(host);
    log.set_time(time);
    log.set_comment(comment);
  }
  void deserialize (const cta::objectstore::serializers::CreationLog & log) {
    user.uid=log.user().uid();
    user.gid=log.user().gid();
    host = log.host();
    time  = log.time();
    comment = log.comment();
  }
};
  
}}
