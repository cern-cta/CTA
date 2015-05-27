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

#include <string>
#include <stdint.h>

namespace cta { namespace objectstore {

class UserIdentity {
public:
  UserIdentity (uint32_t ui, const std::string & un,
    uint32_t gi, const std::string & gn,
    const std::string & hn, uint64_t t,
    const std::string & c): uid(ui), uname(un), gid(gi), gname(gn) {}
  uint32_t uid;
  std::string uname;
  uint32_t gid;
  std::string gname;
};  
  
}}
