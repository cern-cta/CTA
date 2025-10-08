/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummySchemaCatalogue.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

SchemaVersion DummySchemaCatalogue::getSchemaVersion() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummySchemaCatalogue::verifySchemaVersion() {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummySchemaCatalogue::ping() {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
