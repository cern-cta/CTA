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

#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

/**
 * Exception representing an unexpected mismatch between file sizes.
 */
class FileSizeMismatch: public exception::Exception {
public:
  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  FileSizeMismatch(const std::string &context = "", const bool embedBacktrace = true):
    cta::exception::Exception(context, embedBacktrace) {
  }

  /**
   * Destructor.
   */
  ~FileSizeMismatch() override {}
}; // class FileSizeMismatch

} // namespace catalogue
} // namespace cta
