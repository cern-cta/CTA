/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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
/* 
 * File:   SchemaComparer.hpp
 * Author: cedric
 *
 * Created on December 10, 2019, 10:58 AM
 */

#pragma once

#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "catalogue/Catalogue.hpp"

namespace cta {
namespace catalogue {

class SchemaComparer {
public:
  SchemaComparer(cta::rdbms::Conn &connection, cta::rdbms::Login &login, cta::catalogue::Catalogue& catalogue);
  virtual ~SchemaComparer();
  virtual void compare() = 0;
protected:
  cta::rdbms::Conn &m_conn;
  cta::rdbms::Login &m_login;
  cta::catalogue::Catalogue &m_catalogue;
};

}}