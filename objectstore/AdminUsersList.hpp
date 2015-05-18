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

#include "middletier/interface/AdminUser.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {

/**
 * Class containing the list of admin users
 */

class AdminUsersList: public ObjectOps<serializers::AdminUsersList> {
public:
  AdminUsersList(Backend & os);
  
  AdminUsersList(const std::string & name, Backend & os);
  
  void add(const cta::AdminUser & adminUser);
  
  void remove(uint32_t uid, uint32_t gid);
};

}}
