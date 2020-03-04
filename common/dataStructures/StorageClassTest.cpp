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

#include "common/dataStructures/StorageClass.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_common_dataStructures_StorageClassTest : public ::testing::Test {
protected:

  cta_common_dataStructures_StorageClassTest() {
    m_storageClass1DiskInstance1.diskInstance = "disk_instance_1";
    m_storageClass1DiskInstance1.name = "storage_class_1";
    m_storageClass1DiskInstance1.nbCopies = 2;
    m_storageClass1DiskInstance1.comment = "create storage class";

    m_storageClass1DiskInstance2.diskInstance = "disk_instance_2";
    m_storageClass1DiskInstance2.name = "storage_class_1";
    m_storageClass1DiskInstance2.nbCopies = 2;
    m_storageClass1DiskInstance2.comment = "create storage class";

    m_storageClass2DiskInstance1.diskInstance = "disk_instance_1";
    m_storageClass2DiskInstance1.name = "storage_class_2";
    m_storageClass2DiskInstance1.nbCopies = 2;
    m_storageClass2DiskInstance1.comment = "create storage class";

    m_storageClass1DiskInstance1ExactCopy.diskInstance = "disk_instance_1";
    m_storageClass1DiskInstance1ExactCopy.name = "storage_class_1";
    m_storageClass1DiskInstance1ExactCopy.nbCopies = 2;
    m_storageClass1DiskInstance1ExactCopy.comment = "create storage class";

    // Different ignored values
    m_storageClass1DiskInstance1EffectiveCopy.diskInstance = "disk_instance_1";
    m_storageClass1DiskInstance1EffectiveCopy.name = "storage_class_1";
    m_storageClass1DiskInstance1EffectiveCopy.nbCopies = 2;
    m_storageClass1DiskInstance1EffectiveCopy.comment = "create storage class";
    m_storageClass1DiskInstance1EffectiveCopy.nbCopies = 1234;
    m_storageClass1DiskInstance1EffectiveCopy.creationLog.username = "different_creation_username";
    m_storageClass1DiskInstance1EffectiveCopy.creationLog.host = "different_creation_host";
    m_storageClass1DiskInstance1EffectiveCopy.creationLog.time = 5678;
    m_storageClass1DiskInstance1EffectiveCopy.lastModificationLog.username = "different_modification_username";
    m_storageClass1DiskInstance1EffectiveCopy.lastModificationLog.host = "different_modification_host";
    m_storageClass1DiskInstance1EffectiveCopy.lastModificationLog.time = 9012;
    m_storageClass1DiskInstance1EffectiveCopy.comment = "Different comment";
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  cta::common::dataStructures::StorageClass m_storageClass1DiskInstance1;
  cta::common::dataStructures::StorageClass m_storageClass1DiskInstance2;
  cta::common::dataStructures::StorageClass m_storageClass2DiskInstance1;
  cta::common::dataStructures::StorageClass m_storageClass1DiskInstance1ExactCopy;

  /**
   * Different ignored values.
   */
  cta::common::dataStructures::StorageClass m_storageClass1DiskInstance1EffectiveCopy;
};

TEST_F(cta_common_dataStructures_StorageClassTest, copy_constructor) {
  using namespace cta::common::dataStructures;
}

TEST_F(cta_common_dataStructures_StorageClassTest, equality_same_object) {
  using namespace cta::common::dataStructures;

  ASSERT_EQ(m_storageClass1DiskInstance1, m_storageClass1DiskInstance1);
}

TEST_F(cta_common_dataStructures_StorageClassTest, equality_different_objects_exact_same_member_values) {
  using namespace cta::common::dataStructures;

  ASSERT_EQ(m_storageClass1DiskInstance1, m_storageClass1DiskInstance1ExactCopy);
}

TEST_F(cta_common_dataStructures_StorageClassTest, equality_different_objects_different_ignored_values) {
  using namespace cta::common::dataStructures;

  ASSERT_EQ(m_storageClass1DiskInstance1, m_storageClass1DiskInstance1EffectiveCopy);
}

TEST_F(cta_common_dataStructures_StorageClassTest, inequality_different_name) {
  using namespace cta::common::dataStructures;

  ASSERT_NE(m_storageClass1DiskInstance1, m_storageClass2DiskInstance1);
}

} // namespace unitTests
