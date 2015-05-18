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

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {
  
#define MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(A) \
  template <> const serializers::ObjectType ObjectOps<serializers::A>::typeId = serializers::A##_t
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(GenericObject);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(RootEntry);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(AgentRegister);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(Agent);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(JobPool);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(RecallFIFO);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(MigrationFIFO);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(RecallJob);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(Counter);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(FIFO);
  MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID(AdminUsersList);
  
#undef MAKE_CTA_OBJECTSTORE_OBJECTOPS_TYPEID
}} 