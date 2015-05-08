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

#include "AdminUsersList.hpp"

namespace cta { namespace objectstore {
  
AdminUsersList::AdminUsersList(Backend& os):
  ObjectOps<serializers::AdminUsersList>(os) {}

AdminUsersList::AdminUsersList(const std::string & name, Backend& os):
  ObjectOps<serializers::AdminUsersList>(os, name) {}

void AdminUsersList::add(const cta::AdminUser& adminUser) {
  // Check that the admin user is not already present
  ::google::protobuf::RepeatedPtrField<serializers::AdminUser>* list =
      m_payload.mutable_element();
  for (size_t i=0; i<(size_t)list->size(); i++) {
    if (adminUser.getUser().getUid() == list->Get(i).user().uid()) {
      throw cta::exception::Exception("Attempt to duplicate entry in AdminUsersList");
    }
  }
  // Insert the new admin user
  serializers::AdminUser * newEntry = list->Add();
  // Set the content of the new entry
  newEntry->mutable_user()->set_uid(adminUser.getUser().getUid());
  newEntry->mutable_user()->set_gid(adminUser.getUser().getUid());
  newEntry->mutable_creator()->set_uid(adminUser.getCreator().getUid());
  newEntry->mutable_creator()->set_gid(adminUser.getCreator().getGid());
  newEntry->set_creationtime(adminUser.getCreationTime());
  newEntry->set_comment(adminUser.getComment());
}

}}