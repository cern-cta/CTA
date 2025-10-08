/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

#include "common/exception/InvalidArgument.hpp"

namespace cta::common::dataStructures {

struct Label {
  enum class Format : std::uint8_t {
    CTA = 0x00,
    OSM = 0x01,
    Enstore = 0x02,
    EnstoreLarge = 0x03
  };

  static Format validateFormat(const std::optional<std::uint8_t>& ouiFormat, const std::string& strContext) {
    return validateFormat(ouiFormat.value_or(static_cast<uint8_t>(Format::CTA)), strContext);
  }

  static Format validateFormat(const std::uint8_t uiFormat, const std::string& strContext) {
    Format format = static_cast<Format>(uiFormat);
    switch (format) {
      case Format::CTA:
      case Format::OSM:
      case Format::Enstore:
      case Format::EnstoreLarge:
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

} // namespace cta::common::dataStructures
