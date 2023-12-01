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

#include <limits>
#include <stdint.h>
#include <string>

#include "common/dataStructures/MountPolicy.hpp"
#include "EntryLogSerDeser.hpp"
#include "objectstore/cta.pb.h"

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class MountPolicySerDeser: public cta::common::dataStructures::MountPolicy {
public:
  MountPolicySerDeser (): cta::common::dataStructures::MountPolicy() {}
  explicit MountPolicySerDeser(const cta::common::dataStructures::MountPolicy& mp) : cta::common::dataStructures::MountPolicy(mp) {}

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

} // namespace cta::objectstore
