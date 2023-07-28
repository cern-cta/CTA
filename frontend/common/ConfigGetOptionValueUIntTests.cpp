/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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

#include <gtest/gtest.h>
#include <string>
#include <sstream>

#include "frontend/common/Config.hpp"
#include "tests/TempFile.hpp"

namespace unitTests {

TEST(FrontendConfig, HandlesUInt) {

  TempFile tf;
  std::ostringstream strFile;

  strFile << "cta.val.a 1" << std::endl
          << "cta.val.b -10" << std::endl
          << "cta.val.c abc123" << std::endl
          << "cta.val.d 123abc" << std::endl
          << "cta.val.e abc123abc" << std::endl
          << "cta.val.f = 123" << std::endl
          << "cta.val.g " << std::numeric_limits<uint32_t>::max() << std::endl
          << "cta.val.h " << std::numeric_limits<uint32_t>::max() << "0" << std::endl
          << "cta.val.i " << std::numeric_limits<uint64_t>::max() << std::endl;

  tf.stringFill(strFile.str());

  cta::frontend::Config config(tf.path());
  
  EXPECT_EQ(1, config.getOptionValueUInt("cta.val.a"));
  EXPECT_THROW(config.getOptionValueUInt("cta.val.b"), std::out_of_range);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.c"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.d"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.e"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.f"), std::invalid_argument);
  EXPECT_EQ(std::numeric_limits<uint32_t>::max(), config.getOptionValueUInt("cta.val.g"));
  EXPECT_THROW(config.getOptionValueUInt("cta.val.h"), std::out_of_range);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.i"), std::out_of_range);

}

} // namespace unitTests

