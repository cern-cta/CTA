// ----------------------------------------------------------------------
// File: File/StructuresTest.cc
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Structures.hh"

namespace UnitTests {
  TEST(FILE_Structures, VOL1) {
    Tape::AULFile::VOL1 vol1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof(vol1Label));
    /* test that verify throws exceptions */    
    EXPECT_ANY_THROW({
      vol1Label.verify();
    });     
    vol1Label.fill("test");
    ASSERT_NO_THROW({
      vol1Label.verify();
    });
    ASSERT_EQ("test  ",vol1Label.getVSN());
  }
}
