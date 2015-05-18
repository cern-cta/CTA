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

#include "middletier/sharedtest/MiddleTierAbstractTest.hpp"
#include "middletier/interface/MiddleTierAdmin.hpp"
#include "middletier/interface/MiddleTierUser.hpp"
#include "middletier/objectstore/ObjectStoreMiddleTierAdmin.hpp"
#include "middletier/objectstore/ObjectStoreMiddleTierUser.hpp"
#include "objectstore/BackendVFS.hpp"

namespace unitTests {

class OStoreVfsMiddleTier: public localMiddleTier {
public:
  OStoreVfsMiddleTier(): m_vfs(), m_admin(m_vfs), m_user(m_vfs) {}
  virtual cta::MiddleTierAdmin & admin () { return m_admin; }
  virtual cta::MiddleTierUser & user () { return m_user; }
private:
  cta::objectstore::BackendVFS m_vfs;
  cta::OStoreMiddleTierAdmin m_admin;
  cta::OStoreMiddleTierUser m_user;
};

class OStoreVfsMiddleTierFactory: public MiddleTierFactory {
public:
  OStoreVfsMiddleTierFactory() {
    m_localMiddleTier.reset(allocateLocalMiddleTier());
  }
  virtual localMiddleTier * allocateLocalMiddleTier() { 
    return new OStoreVfsMiddleTier; }
} g_OStoreMiddleTierFactory;

// Macro chokes on implicit casting of pointer so we have to do it ourselves
INSTANTIATE_TEST_CASE_P(MiddleTierOStore, MiddleTierAbstractTest, ::testing::Values(
    (MiddleTierFactory*)&g_OStoreMiddleTierFactory));

}