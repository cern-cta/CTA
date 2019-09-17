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

#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "common/make_unique.hpp"

#include <gtest/gtest.h>
#include <set>

namespace unitTests {

class cta_catalogue_TapeItemWrittenPointerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_TapeItemWrittenPointerTest, check_set_order_after_set_fseq_using_unique_ptr) {
  using namespace cta::catalogue;

  std::set<TapeItemWrittenPointer> filesWrittenSet;

  auto file1WrittenUP = cta::make_unique<TapeFileWritten>();
  auto file2WrittenUP = cta::make_unique<TapeFileWritten>();

  file1WrittenUP->fSeq = 1;
  filesWrittenSet.insert(file1WrittenUP.release());

  file2WrittenUP->fSeq = 2;
  filesWrittenSet.insert(file2WrittenUP.release());

  ASSERT_EQ(2, filesWrittenSet.size());

  uint64_t expectedFSeq = 1;
  for(const auto &event: filesWrittenSet) {
    ASSERT_EQ(expectedFSeq, event->fSeq);
    expectedFSeq++;
  }
}

TEST_F(cta_catalogue_TapeItemWrittenPointerTest, DISABLED_check_set_order_after_set_fseq_using_reference) {
  using namespace cta::catalogue;

  std::set<TapeItemWrittenPointer> filesWrittenSet;

  auto file1WrittenUP = cta::make_unique<TapeFileWritten>();
  auto file2WrittenUP = cta::make_unique<TapeFileWritten>();

  auto file1WrittenPtr = file1WrittenUP.get();
  auto file2WrittenPtr = file2WrittenUP.get();

  auto & file1Written = *file1WrittenUP;
  filesWrittenSet.insert(file1WrittenUP.release());
  file1Written.fSeq = 1;

  auto & file2Written = *file2WrittenUP;
  filesWrittenSet.insert(file2WrittenUP.release());
  file2Written.fSeq = 2;

  ASSERT_LT(file1Written, file2Written);

  ASSERT_EQ(2, filesWrittenSet.size());

  // Check the set contains the original objects
  for(const auto &event: filesWrittenSet) {
    ASSERT_TRUE(event.get() == file1WrittenPtr || event.get() == file2WrittenPtr);

    if(event.get() == file1WrittenPtr) {
      ASSERT_EQ(1, event->fSeq);
    } else {
      ASSERT_EQ(2, event->fSeq);
    }
  }

  // Check the order of the set
  uint64_t expectedFSeq = 1;
  for(const auto &event: filesWrittenSet) {
    ASSERT_EQ(expectedFSeq, event->fSeq);
    expectedFSeq++;
  }
}

TEST_F(cta_catalogue_TapeItemWrittenPointerTest, check_set_order_after_set_fseq_using_reference_delayed_insert) {
  using namespace cta::catalogue;

  std::set<TapeItemWrittenPointer> filesWrittenSet;

  auto file1WrittenUP = cta::make_unique<TapeFileWritten>();
  auto file2WrittenUP = cta::make_unique<TapeFileWritten>();

  auto file1WrittenPtr = file1WrittenUP.get();
  auto file2WrittenPtr = file2WrittenUP.get();

  auto & file1Written = *file1WrittenUP;
  file1Written.fSeq = 1;

  auto & file2Written = *file2WrittenUP;
  file2Written.fSeq = 2;

  filesWrittenSet.insert(file1WrittenUP.release());
  filesWrittenSet.insert(file2WrittenUP.release());

  ASSERT_LT(file1Written, file2Written);

  ASSERT_EQ(2, filesWrittenSet.size());

  // Check the set contains the original objects
  for(const auto &event: filesWrittenSet) {
    ASSERT_TRUE(event.get() == file1WrittenPtr || event.get() == file2WrittenPtr);

    if(event.get() == file1WrittenPtr) {
      ASSERT_EQ(1, event->fSeq);
    } else {
      ASSERT_EQ(2, event->fSeq);
    }
  }

  // Check the order of the set
  uint64_t expectedFSeq = 1;
  for(const auto &event: filesWrittenSet) {
    ASSERT_EQ(expectedFSeq, event->fSeq);
    expectedFSeq++;
  }
}

} // namespace unitTests
