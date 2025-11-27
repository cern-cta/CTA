/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskReporterFactory.hpp"

#include "EOSReporter.hpp"
#include "NullReporter.hpp"
#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"

namespace cta::disk {

std::unique_ptr<DiskReporter> DiskReporterFactory::createDiskReporter(const std::string& URL) {
  threading::MutexLocker ml(m_mutex);
  auto regexResult = m_EosUrlRegex.exec(URL);
  if (regexResult.size()) {
    return std::make_unique<EOSReporter>(regexResult[1], regexResult[2]);
  }
  regexResult = m_NullRegex.exec(URL);
  if (regexResult.size()) {
    return std::make_unique<NullReporter>();
  }
  throw cta::exception::Exception(std::string("In DiskReporterFactory::createDiskReporter failed to parse URL: ")
                                  + URL);
}

}  // namespace cta::disk
