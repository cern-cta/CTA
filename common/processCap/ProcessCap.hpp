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

#include <string>
#include <sys/capability.h>

namespace cta::server {

/**
 * Class providing support for UNIX capabilities.
 *
 * This class is used to provide support for UNIX capbilities, so that
 * subclasses can be created that override its virtual member functions.
 * Unit testing is the primary use-case where you may want a dummy capabilities
 * object that does nothing.
 *
 * Please note that process capabilities are not supported on Mac OS X.
 */
class ProcessCap {
public:
  /**
   * Destructor
   */
  virtual ~ProcessCap() = default;

  /**
   * C++ wrapper around the C functions cap_get_proc() and cap_to_text().
   *
   * @return The string representation the capabilities of the current
   * process.
   */
  virtual std::string getProcText();

  /**
   * C++ wrapper around the C functions cap_from_text() and cap_set_proc().
   *
   * @text The string representation the capabilities that the current
   * process should have.
   */
  virtual void setProcText(const std::string &text);

private:
  /**
   * C++ wrapper around the C function cap_get_proc().
   *
   * @return The capability state.
   */
  cap_t getProc();

  /**
   * C++ wrapper around the C function cap_to_text().
   *
   * @param cap The capability state.
   */
  std::string toText(const cap_t cap);

  /**
   * C++ wrapper around the C function cap_from_text().
   *
   * @return The capability state.
   */
  cap_t fromText(const std::string &text);

  /**
   * C++ wrapper around the C function cap_set_proc().
   *
   * @param cap The capability state.
   */
  void setProc(const cap_t cap);
};

} // namespace cta::server
