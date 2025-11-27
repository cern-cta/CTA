/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DiskReporter.hpp"
#include "common/process/threading/Mutex.hpp"
#include "common/utils/Regex.hpp"

#include <future>
#include <string>

namespace cta::disk {

class DiskReporterFactory {
public:
  std::unique_ptr<DiskReporter> createDiskReporter(const std::string& URL);

private:
  // The typical call to give report to EOS will be:
  // xrdfs localhost query opaquefile "/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0"
  // which will be encoded as eosQuery://eosserver.cern.ch/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0"
  // which itself will be translated into (roughly) :
  // XrdCl::FileSystem(XrdCl::URL("eoserver.cern.ch")).Query("/eos/wfe/passwd?mgm.pcmd=event&mgm.fid=112&mgm.logid=cta&mgm.event=migrated&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0");
  cta::utils::Regex m_EosUrlRegex {"^eosQuery://([^/]+)(/.*)$"};
  cta::utils::Regex m_NullRegex {"^$|^null:"};
  /// This mutex ensures we do not use the regexes in parallel.
  cta::threading::Mutex m_mutex;
};
}  // namespace cta::disk
