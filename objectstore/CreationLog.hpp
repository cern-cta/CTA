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
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {

class CreationLog {
public:
  CreationLog (): uid(std::numeric_limits<decltype(uid)>::max()),
          gid(std::numeric_limits<decltype(gid)>::max()),
          time(std::numeric_limits<decltype(CreationLog::time)>::max()) {}
  CreationLog (uint32_t ui, const std::string & un,
    uint32_t gi, const std::string & gn,
    const std::string & hn, uint64_t t,
    const std::string & c): uid(ui), uname(un), gid(gi), gname(gn),
    hostname(hn), time(t), comment(c) {}
  uint32_t uid;
  std::string uname;
  uint32_t gid;
  std::string gname;
  std::string hostname;
  uint64_t time;
  std::string comment;
  void serialize (cta::objectstore::serializers::CreationLog & log) const {
    log.mutable_user()->set_uid(uid);
    log.mutable_user()->set_uname(uname);
    log.mutable_user()->set_gid(gid);
    log.mutable_user()->set_gname(gname);
    log.set_host(hostname);
    log.set_time(time);
    log.set_comment(comment);
  }
  void deserialize (const cta::objectstore::serializers::CreationLog & log) {
    uid   = log.user().uid();
    uname = log.user().uname();
    gid   = log.user().gid();
    gname = log.user().gname();
    hostname = log.host();
    time  = log.time();
    comment = log.comment();
  }
};
  
}}
