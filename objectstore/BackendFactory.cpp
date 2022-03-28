/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
