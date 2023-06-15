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

#pragma once

#include "common/log/LogContext.hpp"
#include "tapeserver/castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"

#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

namespace detail {
//nameholder
struct Recall {};

struct Migration {};

// Enum describing the type of client. Some clients need batched reports,
// some prefer reports file by file
enum ReportBatching { ReportInBulk, ReportByFile };
}  // namespace detail

// Forward declaration to avoid circular inclusions.
class TaskWatchDog;

/**
 * Utility class that should be inherited privately/protectedly 
 * the type PlaceHolder is either detail::Recall or detail::Migration
 */
template<class PlaceHolder>
class ReportPackerInterface {
public:
  // Pass a reference to the watchdog for initial process reporting.
  void setWatchdog(TaskWatchDog& wd) { m_watchdog = &wd; }

protected:
  virtual ~ReportPackerInterface() = default;

  explicit ReportPackerInterface(const cta::log::LogContext& lc) :
  m_lc(lc),
  m_reportBatching(detail::ReportInBulk),
  m_watchdog(nullptr) {}

  /**
   * Log a set of files independently of the success/failure 
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
  template<class C>
  void logReport(const C& c, const std::string& msg) {
    using cta::log::LogContext;
    using cta::log::Param;
    for (typename C::const_iterator it = c.begin(); it != c.end(); ++it) {
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("fileId", (*it)->fileid())
        .add("NSFSEQ", (*it)->fseq())
        .add("NSHOST", (*it)->nshost())
        .add("NSFILETRANSACTIONID", (*it)->fileTransactionId());
      m_lc.log(cta::log::INFO, msg);
    }
  }

  /**
   * Log a set of files independently of the success/failure 
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
  template<class C>
  void logReportWithError(const C& c, const std::string& msg) {
    using cta::log::LogContext;
    using cta::log::Param;
    for (typename C::const_iterator it = c.begin(); it != c.end(); ++it) {
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("fileId", (*it)->fileid())
        .add("NSFSEQ", (*it)->fseq())
        .add("NSHOST", (*it)->nshost())
        .add("NSFILETRANSACTIONID", (*it)->fileTransactionId())
        .add("ErrorMessage", (*it)->errorMessage());
      m_lc.log(cta::log::INFO, msg);
    }
  }

  /**
   * The  log context, copied due to threads
   */
  cta::log::LogContext m_lc;

  /**
   * Define how we should report to the client (by file/in bulk).
   */
  enum detail::ReportBatching m_reportBatching;

public:
  /**
   * Turn off the packing of the reports by the report packer.
   * This is used for recalls driven by read_tp.
   */
  virtual void disableBulk() { m_reportBatching = detail::ReportByFile; }

  /**
   * Pointer to the watchdog, so we can communicate communication errors
   * and end of session results to the initial process
   */
  TaskWatchDog* m_watchdog;
};

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
