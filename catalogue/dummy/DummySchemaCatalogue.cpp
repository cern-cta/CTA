/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummySchemaCatalogue.hpp"

#include "catalogue/SchemaVersion.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

SchemaVersion DummySchemaCatalogue::getSchemaVersion() const {
  throw exception::NotImplementedException();
}

void DummySchemaCatalogue::verifySchemaVersion() {
  throw exception::NotImplementedException();
}

void DummySchemaCatalogue::ping() {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
