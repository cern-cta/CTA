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

#include "TapeItemWritten.hpp"
#include <memory>

namespace cta {
namespace catalogue {
/**
 * A utility struct allowing sorting unique_ptr according to content instead of pointer value
 */
struct TapeItemWrittenPointer: public std::unique_ptr<TapeItemWritten> {
  template<typename ... Ts> TapeItemWrittenPointer(Ts...args): std::unique_ptr<TapeItemWritten>::unique_ptr<TapeItemWritten>(args...) {}
};

// This operator (and not the member one) should be defined to be picked up by std::less,
// which will define the ordering of std::set<TapeItemWritenPointer>.
bool operator<(const TapeItemWrittenPointer& a, const TapeItemWrittenPointer& b);// { return *a < *b; }

} // namespace catalogue
} // namespace cta