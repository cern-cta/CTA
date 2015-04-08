#pragma once

#include "cta/AdminUser.hpp"
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
};

}}
