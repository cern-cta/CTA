/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <limits>
#include <stdint.h>
#include <string>

#include "common/dataStructures/MountPolicy.hpp"
#include "EntryLogSerDeser.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {
/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class MountPolicySerDeser: public cta::common::dataStructures::MountPolicy {
public:
  MountPolicySerDeser (): cta::common::dataStructures::MountPolicy() {}
  MountPolicySerDeser (const cta::common::dataStructures::MountPolicy & mp): cta::common::dataStructures::MountPolicy(mp) {}
  operator cta::common::dataStructures::MountPolicy() {
    return cta::common::dataStructures::MountPolicy(*this);
  }
  void serialize (cta::objectstore::serializers::MountPolicy & osmp) const {
    osmp.set_name(name);
    osmp.set_archivepriority(archivePriority);
    osmp.set_archiveminrequestage(archiveMinRequestAge);
    osmp.set_retrievepriority(retrievePriority);
    osmp.set_retieveminrequestage(retrieveMinRequestAge);
    EntryLogSerDeser(creationLog).serialize(*osmp.mutable_creationlog());
    EntryLogSerDeser(lastModificationLog).serialize(*osmp.mutable_lastmodificationlog());
    osmp.set_comment(comment);
  }
  void deserialize (const cta::objectstore::serializers::MountPolicy & osmp) {
    name=osmp.name();
    archivePriority=osmp.archivepriority();
    archiveMinRequestAge=osmp.archiveminrequestage();
    retrievePriority=osmp.retrievepriority();
    retrieveMinRequestAge=osmp.retieveminrequestage();
    EntryLogSerDeser el;
    el.deserialize(osmp.creationlog());
    creationLog=el;
    el.deserialize(osmp.lastmodificationlog());
    lastModificationLog=el;
    comment=osmp.comment();
  }
};

}}
