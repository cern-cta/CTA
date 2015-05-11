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

#include "MiddleTierAdminAbstractTest.hpp"
#include "ObjectStoreMiddleTierAdmin.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendVFS.hpp"
#include "middletier/cta/SqliteMiddleTierAdmin.hpp"


#define TEST_RADOS 0
#define TEST_VFS 0
#define TEST_SQL 1

namespace unitTests {

#if TEST_RADOS
cta::objectstore::BackendRados osRados("tapetest", "tapetest");
cta::OStoreMiddleTierAdmin mtaRados(osRados);
unitTests::MiddleTierFull middleTierRados;
middleTierRados.admin = &mtaRados;
middleTierRados.user = NULL;

INSTANTIATE_TEST_CASE_P(MiddleTierRados, MiddleTierAdminAbstractTest , ::testing::Values(middleTierRados));
#endif

#if TEST_VFS
cta::objectstore::BackendVFS osVFS;

#endif

#if TEST_SQL
cta::SqliteDatabase sqlite;
cta::Vfs vfs;
cta::SqliteMiddleTierAdmin mtaSQL(vfs, sqlite);
MiddleTierFull mtfSQL(&mtaSQL, NULL);
INSTANTIATE_TEST_CASE_P(MiddleTierSQL, MiddleTierAdminAbstractTest, ::testing::Values(mtfSQL));
#endif

}