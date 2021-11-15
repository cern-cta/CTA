/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string>
#include <vector>

#include "BackendFactory.hpp"
#include "BackendRados.hpp"
#include "BackendVFS.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"


auto cta::objectstore::BackendFactory::createBackend(const std::string& URL, log::Logger & logger)
  -> std::unique_ptr<Backend> {
  utils::Regex fileRe("^file://(.*)$"),
      radosRe("^rados://([^@]+)@([^:]+)(|:(.+))$");
  std::vector<std::string> regexResult;
  // Is it a file:// URL?
  regexResult = fileRe.exec(URL);
  if (regexResult.size()) {
    return std::unique_ptr<Backend>(new BackendVFS(regexResult[1]));
  }
  // Is it a rados:// URL?
  regexResult = radosRe.exec(URL);
  if (regexResult.size()) {
    if (regexResult.size() != 5 && regexResult.size() != 4)
      throw cta::exception::Exception("In BackendFactory::createBackend(): unexpected number of matches in regex");
    if (regexResult.size() == 5)
      return std::unique_ptr<Backend>(new BackendRados(logger, regexResult[1], regexResult[2], regexResult[4]));
    else
      return std::unique_ptr<Backend>(new BackendRados(logger, regexResult[1], regexResult[2]));
  }
  // Fall back to a file URL if all failed
  return std::unique_ptr<Backend>(new BackendVFS(URL));
}
