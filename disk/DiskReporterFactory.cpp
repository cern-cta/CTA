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

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "DiskReporterFactory.hpp"
#include "EOSReporter.hpp"
#include "NullReporter.hpp"

namespace cta {
namespace disk {

DiskReporter* DiskReporterFactory::createDiskReporter(const std::string& URL) {
  threading::MutexLocker ml(m_mutex);
  auto regexResult = m_EosUrlRegex.exec(URL);
  if (regexResult.size()) {
    return new EOSReporter(regexResult[1], regexResult[2]);
  }
  regexResult = m_NullRegex.exec(URL);
  if (regexResult.size()) {
    return new NullReporter();
  }
  throw cta::exception::Exception(std::string("In DiskReporterFactory::createDiskReporter failed to parse URL: ") +
                                  URL);
}

}  // namespace disk
}  // namespace cta
