/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"

#include <gtest/gtest.h>
#include <set>

namespace unitTests {

class cta_catalogue_TapeItemWrittenPointerTest : public ::testing::Test {
protected:

  void SetUp() final {
    // nothing to do
  }

  void TearDown() final {
    // nothing to do
  }
};

TEST_F(cta_catalogue_TapeItemWrittenPointerTest, check_set_order_after_set_fseq_using_unique_ptr) {
  using namespace cta::catalogue;

  std::set<TapeItemWrittenPointer> filesWrittenSet;

  auto file1WrittenUP = std::make_unique<TapeFileWritten>();
  auto file2WrittenUP = std::make_unique<TapeFileWritten>();

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

  auto file1WrittenUP = std::make_unique<TapeFileWritten>();
  auto file2WrittenUP = std::make_unique<TapeFileWritten>();

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

  auto file1WrittenUP = std::make_unique<TapeFileWritten>();
  auto file2WrittenUP = std::make_unique<TapeFileWritten>();

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
