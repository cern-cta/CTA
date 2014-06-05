/******************************************************************************
 *                      CephGlobals.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * couple of global maps needed for interfacing with ceph libraries.
 * Basically a cache of connections to ceph
 *
 * @author Dennis Waldron
 *****************************************************************************/

#include "castor/gc/CephGlobals.hpp"
#include <map>

/// global variables holding ioCtx and stripers for each ceph pool
std::map<std::string, libradosstriper::RadosStriper*> g_radosStripers;
std::map<std::string, librados::IoCtx*> g_ioCtx;

librados::IoCtx* castor::gc::getRadosIoCtx(std::string pool) {
  libradosstriper::RadosStriper* striper = castor::gc::getRadosStriper(pool);
  if (0 == striper) return 0;
  return g_ioCtx[pool];
}

libradosstriper::RadosStriper* castor::gc::getRadosStriper(std::string pool) {
  std::map<std::string, libradosstriper::RadosStriper*>::iterator it =
    g_radosStripers.find(pool);
  if (it == g_radosStripers.end()) {
    // we need to create a new radosStriper
    librados::Rados cluster;
    int rc = cluster.init(0);
    if (rc) return 0;
    rc = cluster.conf_read_file(NULL);
    if (rc) {
      cluster.shutdown();
      return 0;
    }
    cluster.conf_parse_env(NULL);
    rc = cluster.connect();
    if (rc) {
      cluster.shutdown();
      return 0;
    }
    g_ioCtx[pool] = new librados::IoCtx();
    librados::IoCtx *ioctx = g_ioCtx[pool];
    rc = cluster.ioctx_create(pool.c_str(), *ioctx);
    if (rc != 0) {
      g_ioCtx.erase(pool);
      return 0;
    }
    libradosstriper::RadosStriper *newStriper = new libradosstriper::RadosStriper;
    rc = libradosstriper::RadosStriper::striper_create(*ioctx, newStriper);
    if (rc != 0) {
      delete newStriper;
      g_ioCtx.erase(pool);
      return 0;
    }
    it = g_radosStripers.insert(std::pair<std::string, libradosstriper::RadosStriper*>
                                (pool, newStriper)).first;
  }
  return it->second;
}
