/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "ObjectOps.hpp"
#include <list>
#include <limits>
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/DriveNextState.hpp"

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class DriveRegister: public ObjectOps<serializers::DriveRegister, serializers::DriveRegister_t> {
public:
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);
  DriveRegister(const std::string & address, Backend & os);
  explicit DriveRegister(GenericObject& go);
  void initialize() override;
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  bool isEmpty();

  /**
   * A drive register entry (drive name + object address)
   */
  struct DriveAddress {
    std::string driveName;
    std::string driveStateAddress;
  };

  // Drives management =========================================================
  /**
   * Returns all the drive states addresses stored in the drive registry.
   * @return a list of all the drive states
   */
  std::list<DriveAddress> getDriveAddresses();

  /**
   * Returns all the drive states addresses stored in the drive registry.
   * @return a list of all the drive states
   */
  std::string getDriveAddress(const std::string & driveName);

  /**
   * Adds a drive status reference to the register.
   * @param driveName
   * @param driveAddress
   */
  void setDriveAddress(const std::string & driveName, const std::string &driveAddress);

  /**
   * Removes entry from drive addresses.
   * @param driveName
   */
  void removeDrive(const std::string & driveName);

  /**
   * JSON dump of the drive register
   * @return
   */
  std::string dump();
};

} // namespace cta::objectstore
