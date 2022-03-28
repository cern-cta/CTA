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

#include "tapeserver/daemon/TpconfigLine.hpp"
#include "common/SourcedParameter.hpp"
#include "common/exception/Exception.hpp"

#include <map>

namespace cta { namespace tape { namespace daemon {

/**
 * A map of lines parsed from a TPCONFIG file (key is the drive name)
 */
class Tpconfig: public std::map<std::string, cta::SourcedParameter<TpconfigLine>> {
public:

  CTA_GENERATE_EXCEPTION_CLASS(InvalidArgument);
  CTA_GENERATE_EXCEPTION_CLASS(DuplicateEntry);
  /**
   * Parses the specified TPCONFIG file.
   *
   * @param filename The filename of the TPCONFIG file.
   * @return The result of parsing the TPCONFIG file.
   */
  static Tpconfig parseFile(const std::string &filename);
}; // class TpconfigLines

}}} // namespace cta::tape::daemon
