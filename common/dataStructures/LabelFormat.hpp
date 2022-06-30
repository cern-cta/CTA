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

#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

#include "common/exception/InvalidArgument.hpp"

namespace cta {
namespace common {
namespace dataStructures {

struct Label {
  enum class Format : std::uint8_t {
    CTA = 0x00,
    OSM = 0x01
  };

  static Format validateFormat(const std::optional<std::uint8_t>& ouiFormat, const std::string& strContext) {
    return validateFormat(ouiFormat.value_or(static_cast<uint8_t>(Format::CTA)), strContext);
  }

  static Format validateFormat(const std::uint8_t uiFormat, const std::string& strContext) {
    Format format = static_cast<Format>(uiFormat);
    switch (format) {
      case Format::CTA:
      case Format::OSM:
        return format;
      default:
      {
        std::ostringstream ex_str;
        ex_str << strContext << ": Unknown label format " << std::showbase << std::internal << std::setfill('0')
                << std::hex << std::setw(4) << static_cast<unsigned int>(uiFormat);
        throw cta::exception::InvalidArgument(ex_str.str());
      }
    }
  }
  // prevent the generation of default public constructor and destructor
protected:
  Label() = default;
  virtual ~Label() = default;
};

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
