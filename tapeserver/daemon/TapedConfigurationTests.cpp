/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include "tapeserver/daemon/ConfigurationFile.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tests/TempFile.hpp"

namespace unitTests {

TEST(cta_Daemon, TapedConfiguration) {
  TempFile incompleteConfFile, completeConfFile;
  incompleteConfFile.stringFill(
  "# My incomplete taped configuration file\n"
  );
  completeConfFile.stringFill(
  "#A good enough configuration file for taped\n"
  "general ObjectStoreURL vfsObjectStore:///tmp/dir\n"
  "general FileCatalogURL sqliteFileCatalog:///tmp/dir2\n"
  );
  ASSERT_THROW(cta::tape::daemon::TapedConfiguration::createFromCtaConf(incompleteConfFile.path()),
      cta::tape::daemon::SourcedParameter<std::string>::MandatoryParameterNotDefined);
  auto completeConfig = 
    cta::tape::daemon::TapedConfiguration::createFromCtaConf(completeConfFile.path());
  ASSERT_EQ(completeConfFile.path()+":2", completeConfig.objectStoreURL.source());
  ASSERT_EQ("vfsObjectStore:///tmp/dir", completeConfig.objectStoreURL.value());
  ASSERT_EQ(completeConfFile.path()+":3", completeConfig.fileCatalogURL.source());
  ASSERT_EQ("sqliteFileCatalog:///tmp/dir2", completeConfig.fileCatalogURL.value());
}

} // namespace unitTests