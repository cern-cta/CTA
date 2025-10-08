/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class RepackIndex: public ObjectOps<serializers::RepackIndex, serializers::RepackIndex_t> {
public:
  explicit RepackIndex(Backend& os);
  RepackIndex(const std::string& address, Backend& os);
  explicit RepackIndex(GenericObject& go);
  void initialize() override;
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  bool isEmpty();

  /**
   * A repack tape register entry (vid + object address)
   */
  struct RepackRequestAddress {
    std::string vid;
    std::string repackRequestAddress;
  };

  // Repack tapes management =========================================================
  /**
   * Returns all the repack tape states addresses stored in the drive registry.
   * @return a list of all the drive states
   */
  std::list<RepackRequestAddress> getRepackRequestsAddresses();

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchVID);
  /**
   * Returns the repack tape address for the given vid.
   * @return the object address
   */
  std::string getRepackRequestAddress(const std::string & vid);

  /**
   * Adds a drive status reference to the register. Throws an exception if a request is already recorded for this VID.
   * @param driveName
   * @param driveAddress
   */
  void addRepackRequestAddress(const std::string & vid, const std::string &repackRequestAddress);

  CTA_GENERATE_EXCEPTION_CLASS(VidAlreadyRegistered);

  /**
   * Removes entry from drive addresses.
   * @param driveName
   */
  void removeRepackRequest(const std::string & vid);

  /**
   * JSON dump of the index
   * @return
   */
  std::string dump();
};

} // namespace cta::objectstore
