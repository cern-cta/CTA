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

#pragma once

#include "common/UserIdentity.hpp"
#include "objectstore/cta.pb.h"
#include "common/dataStructures/EntryLog.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta { namespace objectstore {
/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class EntryLog: public cta::common::dataStructures::EntryLog {
public:
  EntryLog (): cta::common::dataStructures::EntryLog() {}
  EntryLog (const cta::common::dataStructures::EntryLog & el): cta::common::dataStructures::EntryLog(el) {}
  EntryLog (const cta::common::dataStructures::UserIdentity& u, const std::string & hn, uint64_t t): cta::common::dataStructures::EntryLog() {
    user=u;
    host=hn;
    time=t;
  }
  operator cta::common::dataStructures::EntryLog() {
    return cta::common::dataStructures::EntryLog(*this);
  } 
  void serialize (cta::objectstore::serializers::CreationLog & log) const {
    log.mutable_user()->set_name(user.name);
    log.mutable_user()->set_group(user.group);
    log.set_host(host);
    log.set_time(time);
  }
  void deserialize (const cta::objectstore::serializers::CreationLog & log) {
    user.name=log.user().name();
    user.group=log.user().group();
    host = log.host();
    time  = log.time();
  }
};
  
}}
