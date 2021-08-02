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

#include "common/processCap/ProcessCap.hpp"

#include <string>
#include <sys/capability.h>

namespace cta {
namespace server  {

/**
 * A dummy class that pretends to provide support for UNIX capabilities.
 *
 * This primary goal of this class is to facilitate unit testing.
 */
class ProcessCapDummy: public ProcessCap {
public:

  /**
   * Destructor.
   */
  ~ProcessCapDummy() throw();

  /**
   * C++ wrapper around the C functions cap_get_proc() and cap_to_text().
   *
   * @return The string representation the capabilities of the current
   * process.
   */
  std::string getProcText();

  /**
   * C++ wrapper around the C functions cap_from_text() and cap_set_proc().
   *
   * @text The string representation the capabilities that the current
   * process should have.
   */
  void setProcText(const std::string &text);

private:

  /**
   * The string representation of the current capability state.
   */
  std::string m_text;

}; // class ProcessCapDummy

} // namespace server
} // namespace cta
