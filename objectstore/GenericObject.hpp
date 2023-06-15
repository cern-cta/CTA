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

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta {
namespace objectstore {

class GenericObject : public ObjectOps<serializers::GenericObject, serializers::GenericObject_t> {
public:
  GenericObject(const std::string& name, Backend& os) :
  ObjectOps<serializers::GenericObject, serializers::GenericObject_t>(os, name) {};

  CTA_GENERATE_EXCEPTION_CLASS(ForbiddenOperation);

  /** This object has a special, relaxed version of header parsing as all types
   * of objects are accepted here. */
  void getHeaderFromObjectData(const std::string& objData) override;

  /** Overload of ObjectOps's implementation: this special object does not really 
   parse its payload */
  void getPayloadFromHeader() override {}

  /** Overload of ObjectOps's implementation: we will leave the payload transparently
   * untouched and only deal with header parameters */
  void commit();

  /** Get the object's type (type is forced implicitly in other classes) */
  serializers::ObjectType type();

  /** Overload of ObjectOps's implementation: this operation is forbidden. Generic
   * Object is only used to manipulate existing objects */
  void insert();

  /** Overload of ObjectOps's implementation: this operation is forbidden. Generic
   * Object is only used to manipulate existing objects */
  void initialize();

  /** Overload of ObjectOps's implementation: this operation is forbidden. Generic
   * Object cannot be garbage collected as-is */
  void garbageCollect(const std::string& presumedOwner,
                      AgentReference& agentReference,
                      log::LogContext& lc,
                      cta::catalogue::Catalogue& catalogue) override;

  /** This dispatcher function will call the object's garbage collection function.
   * It also handles the passed lock and returns is unlocked.
   * The object is expected to be passed exclusive locked and already fetched.
   * No extra care will be required from the object
   *
   * @param lock reference to the generic object's lock
   * @param presumedOwner address of the agent which pointed to the object
   * @param agent reference object allowing creation of new objects when needed (at least for requeuing of requests)
   * @params lc log context passed to garbage collection routines to log GC steps.
   * @params catalogue reference to the catalogue, used by some garbage collection routines (specially for RetrieveRequests
   * which are tape status dependent.
   */
  void garbageCollectDispatcher(ScopedExclusiveLock& lock,
                                const std::string& presumedOwner,
                                AgentReference& agentReference,
                                log::LogContext& lc,
                                cta::catalogue::Catalogue& catalogue);

  /** This dispatcher function will call the object's dump.
   * It also handles the passed lock.
   *
   * @param lock reference to the generic object's lock
   */
  std::string dump();

  CTA_GENERATE_EXCEPTION_CLASS(UnsupportedType);

  /**
   * This method will extract contents of the generic object's header and
   * transfer them to the header of the destination
   * The source (this) will be emptied.
   * @param destination pointed to the new object's destination
   */
  void transplantHeader(ObjectOpsBase& destination);

  /**
   * This method exposes the reference to the object store.
   */
  Backend& objectStore();
};

}  // namespace objectstore
}  // namespace cta
