/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#pragma once

#include <memory>

#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

/**
  * A fully qualified storage class, in other words the name of the disk
  * instance and the name of the storage class.
  */
struct StorageClass {

  /**
    * The name of the storage class which is only guaranteed to be unique
    */
  std::string storageClassName;

  /**
    * Constructor.
    *
    * @param sN The name of the storage class which is only guaranteed to be
    * unique within its disk instance.
    */
  explicit StorageClass(const std::string &s): storageClassName(s) {}

  /**
    * Less than operator.
    *
    * @param rhs The argument on the right hand side of the operator.
    * @return True if this object is less than the argument on the right hand
    * side of the operator.
    */
  bool operator<(const StorageClass &rhs) const {
    return storageClassName < rhs.storageClassName;
  }
}; // struct StorageClass

class RdbmsCatalogue;

class RdbmsStorageClassCatalogue : public StorageClassCatalogue {
public:
  ~RdbmsStorageClassCatalogue() override = default;

  void createStorageClass(
    const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::StorageClass &storageClass) override;

  void deleteStorageClass(const std::string &storageClassName) override;

  std::list<common::dataStructures::StorageClass> getStorageClasses() const override;

  common::dataStructures::StorageClass getStorageClass(const std::string &name) const override;

  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t nbCopies) override;

  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) override;

  void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) override;

protected:
  RdbmsStorageClassCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);

private:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  /**
   * Returns true if the specified storage class is currently being used by one
   * or more archive routes.
   *
   * @param conn The database connection.
   * @param storageClassName The name of the storage class.
   */
  bool storageClassIsUsedByArchiveRoutes(rdbms::Conn &conn, const std::string &storageClassName) const;

  /**
   * Returns true if the specified storage class is currently being used by one
   * or more archive files.
   *
   * @param conn The database connection.
   * @param storageClassName The name of the storage class.
   */
  bool storageClassIsUsedByArchiveFiles(rdbms::Conn &conn, const std::string &storageClassName) const;

  /**
   * Returns true if the specified storage class is currently being used by one
   * or more files in the recycle log.
   *
   * @param conn The database connection.
   * @param storageClassName The name of the storage class.
   */
  bool storageClassIsUsedByFileRecyleLogs(rdbms::Conn & conn, const std::string & storageClassName) const;

  /**
   * Returns a unique storage class ID that can be used by a new storage class
   * within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique storage class ID that can be used by a new storage class
   * within the catalogue.
   */
  virtual uint64_t getNextStorageClassId(rdbms::Conn &conn) = 0;
};

}} // namespace cta::catalogue
