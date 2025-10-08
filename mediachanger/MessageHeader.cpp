/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/MessageHeader.hpp"

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MessageHeader::MessageHeader():
  magic(0),
  reqType(0),
  lenOrStatus(0) {
}

} // namespace cta::mediachanger
