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

#include "BackendFactory.hpp"
#include "BackendRados.hpp"
#include "BackendVFS.hpp"
#include "common/Utils.hpp"
#include "tapeserver/castor/tape/tapeserver/utils/Regex.hpp"

auto cta::objectstore::BackendFactory::createBackend(const std::string& URL)
  -> std::unique_ptr<Backend> {
  castor::tape::utils::Regex fileRe("^file://(.*)$"),
      radosRe("^rados://([^@]+)@(.*)$");
  std::vector<std::string> regexResult;
  // Is it a file:// URL?
  regexResult = fileRe.exec(URL);
  if (regexResult.size()) {
    return std::unique_ptr<Backend>(new BackendVFS(regexResult[1]));
  }
  // Is it a rados:// URL?
  regexResult = radosRe.exec(URL);
  if (regexResult.size()) {
    return std::unique_ptr<Backend>(new BackendRados(regexResult[1], regexResult[2]));
  }
  // Fall back to a file URL if all failed
  return std::unique_ptr<Backend>(new BackendVFS(URL));
}