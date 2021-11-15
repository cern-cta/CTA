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

#pragma once

#include <future>
#include <string>

#include "common/threading/Mutex.hpp"
#include "common/utils/Regex.hpp"
#include "DiskReporter.hpp"

namespace cta {
namespace disk {

class DiskReporterFactory {
 public:
  DiskReporter * createDiskReporter(const std::string& URL);
 private:
  // The typical call to give report to EOS will be:
  // xrdfs localhost query opaquefile "/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0"
  // which will be encoded as eosQuery://eosserver.cern.ch/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0"
  // which itself will be translated into (roughly) :
  // XrdCl::FileSystem(XrdCl::URL("eoserver.cern.ch")).Query("/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0");
  cta::utils::Regex m_EosUrlRegex{"^eosQuery://([^/]+)(/.*)$"};
  cta::utils::Regex m_NullRegex{"^$|^null:"};
  /// This mutex ensures we do not use the regexes in parallel.
  cta::threading::Mutex m_mutex;
};
}  // namespace disk
}  // namespace cta
