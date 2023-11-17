/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class RepackIndex: public ObjectOps<serializers::RepackIndex, serializers::RepackIndex_t> {
public:
  RepackIndex(Backend & os);
  RepackIndex(const std::string & address, Backend & os);
  RepackIndex(GenericObject & go);
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
