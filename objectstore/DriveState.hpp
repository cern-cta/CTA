/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "ObjectOps.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/DriveNextState.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"

namespace cta { namespace objectstore {

class GenericObject;

class DriveState: public ObjectOps<serializers::DriveState, serializers::DriveState_t> {
public:
  // Undefined object constructor
  DriveState(Backend & os);

  // Garbage collection interface
  DriveState(GenericObject & go);
  void garbageCollect(const std::string& presumedOwner, AgentReference& agentReference,
    log::LogContext& lc, cta::catalogue::Catalogue& catalogue) override;

  // Initialization
  void initialize(const std::string & driveName);

  // Standard access constructor
  DriveState(const std::string & address, Backend & os);

  // Data access
  cta::common::dataStructures::DriveState getState();
  void setState(cta::common::dataStructures::DriveState & state);

  std::map<std::string, uint64_t> getDiskSpaceReservations();
  void addDiskSpaceReservation(const std::string & diskSystemName, uint64_t bytes);
  void addDiskSpaceReservation(catalogue::Catalogue* catalogue, const std::string& diskSystemName, uint64_t bytes);
  CTA_GENERATE_EXCEPTION_CLASS(NegativeDiskSpaceReservationReached);
  void substractDiskSpaceReservation(const std::string & diskSystemName, uint64_t bytes);
  void subtractDiskSpaceReservation(catalogue::Catalogue* catalogue, const std::string& diskSystemName, uint64_t bytes);
  void resetDiskSpaceReservation();

  void setConfig(const cta::tape::daemon::TapedConfiguration &tapedConfiguration);
  void setTpConfig(const cta::tape::daemon::TpconfigLine &tpConfigLine);
  /**
   * JSON dump of the drive state
   * @return
   */
  std::string dump();

  void commit();

private:
  /**
   * Allows to set a configuration value to the DriveConfig item passed in parameter
   * @param item the objectstore DriveConfig item
   * @param value the value to set to the item
   */
  template <typename T>
  void setConfigValue(cta::objectstore::serializers::DriveConfig * item,const T& value);

  /**
   * Add a DriveConfig to the DriveState DriveConfig list and return its pointer
   * so the pointed DriveConfig can be modified afterwards
   * @param sourcedParameter the configuration that will be used for initialize the DriveConfig item
   * @return
   */
  template <typename T>
  cta::objectstore::serializers::DriveConfig * createAndInitDriveConfig(cta::SourcedParameter<T>& sourcedParameter);

  /**
   * Allows to put the content of the sourcedParameter passed in parameter
   * into the DriveState's protobuf list of DriveConfig items (used for the cta-admin --json dr ls) command
   * @param sourcedParameter the SourcedParameter to save into the protobuf list of DriveConfig items
   */
  template <typename T>
  void fillConfig(cta::SourcedParameter<T>& sourceParameter);
};

}} // namespace cta::objectstore
