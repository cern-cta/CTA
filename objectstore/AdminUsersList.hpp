#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "libs/middletier/AdminUser.hpp"

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