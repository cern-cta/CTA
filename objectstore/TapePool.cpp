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

#include "TapePool.hpp"

cta::objectstore::TapePool::TapePool(const std::string& address, Backend& os):
  ObjectOps<serializers::TapePool>(os, address) { }

void cta::objectstore::TapePool::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::TapePool>::initialize();
  // Setup the object so it's valid
  m_payload.set_name(name);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}
