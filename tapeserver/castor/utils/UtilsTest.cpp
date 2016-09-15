/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <list>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

namespace unitTests {

class castor_utils : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_utils, testCopyStringNullDst) {
  using namespace castor::utils;
  char dummy[6] = "Dummy";

  ASSERT_THROW(copyString(NULL, 0, dummy),
    cta::exception::Exception);
}

TEST_F(castor_utils, testCopyString) {
  using namespace castor::utils;
  char src[12]  = "Hello World";
  char dst[12];

  copyString(dst, src);
  ASSERT_EQ(0, strcmp(dst, src));
}

} // namespace unitTests
