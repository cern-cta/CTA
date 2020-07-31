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

#include "DriveState.hpp"
#include "GenericObject.hpp"
#include <google/protobuf/util/json_util.h>
#include <version.h>
#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/FetchReportOrFlushLimits.hpp"

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// DriveState::DriveState())
//------------------------------------------------------------------------------
DriveState::DriveState(GenericObject& go):
ObjectOps<serializers::DriveState, serializers::DriveState_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

//------------------------------------------------------------------------------
// DriveState::garbageCollect())
//------------------------------------------------------------------------------
void DriveState::garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc, cta::catalogue::Catalogue& catalogue) {
  // The drive state is easily replaceable. We just delete it on garbage collection.
  checkPayloadWritable();
  if (presumedOwner != m_header.owner())
    return;
  remove();
  log::ScopedParamContainer params(lc);
  params.add("driveStateObject", getAddressIfSet());
  lc.log(log::INFO, "In DriveState::garbageCollect(): Garbage collected and removed drive state object.");
}

//------------------------------------------------------------------------------
// DriveState::initialize())
//------------------------------------------------------------------------------
void DriveState::initialize(const std::string & driveName) {
  // Setup underlying object with defaults from dataStructures::DriveState
  ObjectOps<serializers::DriveState, serializers::DriveState_t>::initialize();
  m_payload.set_drivename(driveName);
  cta::common::dataStructures::DriveState driveState;
  driveState.driveName = driveName;
  driveState.downOrUpStartTime = ::time(nullptr);
  driveState.desiredDriveState.setReasonFromLogMsg(cta::log::INFO,"Drive created.");
  setState(driveState);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// DriveState::DriveState())
//------------------------------------------------------------------------------
DriveState::DriveState(const std::string& address, Backend& os):
  ObjectOps<serializers::DriveState, serializers::DriveState_t>(os, address) { }

//------------------------------------------------------------------------------
// DriveState::DriveState())
//------------------------------------------------------------------------------
DriveState::DriveState(Backend& os):
  ObjectOps<serializers::DriveState, serializers::DriveState_t>(os) { }

//------------------------------------------------------------------------------
// DriveState::getState())
//------------------------------------------------------------------------------
cta::common::dataStructures::DriveState DriveState::getState() {
  cta::common::dataStructures::DriveState ret;
  ret.driveName                   = m_payload.drivename();
  ret.host                        = m_payload.host();
  ret.logicalLibrary              = m_payload.logicallibrary();
  ret.sessionId                   = m_payload.sessionid();
  ret.bytesTransferredInSession   = m_payload.bytestransferedinsession();
  ret.filesTransferredInSession   = m_payload.filestransferedinsession();
  ret.latestBandwidth             = m_payload.latestbandwidth();
  ret.sessionStartTime            = m_payload.sessionstarttime();
  ret.mountStartTime              = m_payload.mountstarttime();
  ret.transferStartTime           = m_payload.transferstarttime();
  ret.unloadStartTime             = m_payload.unloadstarttime();
  ret.unmountStartTime            = m_payload.unmountstarttime();
  ret.drainingStartTime           = m_payload.drainingstarttime();
  ret.downOrUpStartTime           = m_payload.downorupstarttime();
  ret.probeStartTime              = m_payload.probestarttime();
  ret.cleanupStartTime            = m_payload.cleanupstarttime();
  ret.lastUpdateTime              = m_payload.lastupdatetime();
  ret.startStartTime              = m_payload.startstarttime();
  ret.shutdownTime                = m_payload.shutdowntime();
  ret.mountType                   = (common::dataStructures::MountType) m_payload.mounttype();
  ret.driveStatus                 = (common::dataStructures::DriveStatus) m_payload.drivestatus();
  ret.desiredDriveState.up        = m_payload.desiredup();
  ret.desiredDriveState.forceDown = m_payload.desiredforcedown();
  ret.currentVid                  = m_payload.currentvid();
  ret.currentTapePool             = m_payload.currenttapepool();
  ret.currentPriority             = m_payload.current_priority();
  ret.ctaVersion                  = m_payload.cta_version();
  if(m_payload.has_reason()){
    ret.desiredDriveState.reason = m_payload.reason();
  }
  if(m_payload.has_comment()){
    ret.desiredDriveState.comment = m_payload.comment();
  }
  for(auto & driveConfigItem: m_payload.drive_config()){
    ret.driveConfigItems.push_back({driveConfigItem.category(),driveConfigItem.key(),driveConfigItem.value(),driveConfigItem.source()});
  }
  ret.devFileName = m_payload.dev_file_name();
  ret.rawLibrarySlot = m_payload.raw_library_slot();
  if (m_payload.has_current_activity())
    ret.currentActivityAndWeight = 
      cta::common::dataStructures::DriveState::ActivityAndWeight{
        m_payload.current_activity(), m_payload.current_activity_weight()};
  if (m_payload.has_nextmounttype())
    ret.nextMountType = (common::dataStructures::MountType) m_payload.nextmounttype();
  if (m_payload.has_nexttapepool())
    ret.nextTapepool = m_payload.nexttapepool();
  if (m_payload.has_nextvid())
    ret.nextVid = m_payload.nextvid();
  if (m_payload.has_next_priority())
    ret.nextPriority = m_payload.next_priority();
  if (m_payload.has_next_activity())
    ret.nextActivityAndWeight =
        cta::common::dataStructures::DriveState::ActivityAndWeight{
          m_payload.next_activity(), m_payload.next_activity_weight()};
  return ret;
}

//------------------------------------------------------------------------------
// DriveState::setState())
//------------------------------------------------------------------------------
void DriveState::setState(cta::common::dataStructures::DriveState& state) {
  // There should be no need to set the drive name.
  m_payload.set_host(state.host);
  m_payload.set_logicallibrary(state.logicalLibrary);
  m_payload.set_sessionid(state.sessionId);
  m_payload.set_bytestransferedinsession(state.bytesTransferredInSession);
  m_payload.set_filestransferedinsession(state.filesTransferredInSession);
  m_payload.set_latestbandwidth(state.latestBandwidth);
  m_payload.set_sessionstarttime(state.sessionStartTime);
  m_payload.set_mountstarttime(state.mountStartTime);
  m_payload.set_transferstarttime(state.transferStartTime);
  m_payload.set_unloadstarttime(state.unloadStartTime);
  m_payload.set_unmountstarttime(state.unmountStartTime);
  m_payload.set_drainingstarttime(state.drainingStartTime);
  m_payload.set_downorupstarttime(state.downOrUpStartTime);
  m_payload.set_probestarttime(state.probeStartTime);
  m_payload.set_cleanupstarttime(state.cleanupStartTime);
  m_payload.set_lastupdatetime(state.lastUpdateTime);
  m_payload.set_startstarttime(state.startStartTime);
  m_payload.set_shutdowntime(state.shutdownTime);
  m_payload.set_mounttype((uint32_t)state.mountType);
  m_payload.set_drivestatus((uint32_t)state.driveStatus);
  common::dataStructures::DesiredDriveState desiredDriveState = state.desiredDriveState;
  m_payload.set_desiredup(desiredDriveState.up);
  m_payload.set_desiredforcedown(desiredDriveState.forceDown);
  m_payload.set_currentvid(state.currentVid);
  m_payload.set_currenttapepool(state.currentTapePool);
  m_payload.set_current_priority(state.currentPriority);
  cta::optional<std::string> reason = desiredDriveState.reason;
  cta::optional<std::string> comment = desiredDriveState.comment;
  if(reason) {
    if(reason.value().empty()){
      //The only way to erase a reason is to set the optional value to an empty string
      m_payload.clear_reason();
    } else {
      m_payload.set_reason(reason.value());
    }
  }
  if(comment) {
    m_payload.set_comment(comment.value());
  }
  if (state.currentActivityAndWeight) {
    m_payload.set_current_activity(state.currentActivityAndWeight.value().activity);
    m_payload.set_current_activity_weight(state.currentActivityAndWeight.value().weight);
  } else {
    m_payload.clear_current_activity();
    m_payload.clear_current_activity_weight();
  }
  m_payload.set_nextvid(state.nextVid);
  m_payload.set_nexttapepool(state.nextTapepool);
  m_payload.set_next_priority(state.nextPriority);
  m_payload.set_nextmounttype((uint32_t)state.nextMountType);
  if (state.nextActivityAndWeight) {
    m_payload.set_next_activity(state.nextActivityAndWeight.value().activity);
    m_payload.set_next_activity_weight(state.nextActivityAndWeight.value().weight);
  } else {
    m_payload.clear_next_activity();
    m_payload.clear_next_activity_weight();
  }
}

template <>
void DriveState::setConfigValue<std::string>(cta::objectstore::serializers::DriveConfig * item, const std::string& value){
  item->set_value(value);
}

template<>
void DriveState::setConfigValue<uint64_t>(cta::objectstore::serializers::DriveConfig * item,const uint64_t & value){
  item->set_value(std::to_string(value));
}

template<>
void DriveState::setConfigValue<time_t>(cta::objectstore::serializers::DriveConfig * item,const time_t & value){
  item->set_value(std::to_string(value));
}

template <typename T>
cta::objectstore::serializers::DriveConfig * DriveState::createAndInitDriveConfig(cta::SourcedParameter<T>& sourcedParameter) {
  auto item = m_payload.mutable_drive_config()->Add();
  item->set_source(sourcedParameter.source());
  item->set_category(sourcedParameter.category());
  item->set_key(sourcedParameter.key());
  return item;
}

template<>
void DriveState::fillConfig<std::string>(cta::SourcedParameter<std::string> & sourcedParameter){
  auto item = createAndInitDriveConfig(sourcedParameter);
  setConfigValue(item,sourcedParameter.value());
}

template <>
void DriveState::fillConfig<cta::tape::daemon::FetchReportOrFlushLimits>(cta::SourcedParameter<cta::tape::daemon::FetchReportOrFlushLimits>& sourcedParameter){
  auto itemFiles = createAndInitDriveConfig(sourcedParameter);
  std::string key = sourcedParameter.key();
  cta::utils::searchAndReplace(key,"Bytes","");
  cta::utils::searchAndReplace(key,"Files","");
  itemFiles->set_key(key.append("Files"));
  setConfigValue(itemFiles, sourcedParameter.value().maxFiles);
  
  cta::utils::searchAndReplace(key,"Files","");
  auto itemBytes = createAndInitDriveConfig(sourcedParameter);
  itemBytes->set_key(key.append("Bytes"));
  setConfigValue(itemBytes,sourcedParameter.value().maxBytes);
}

template<>
void DriveState::fillConfig<uint64_t>(cta::SourcedParameter<uint64_t>& sourcedParameter){
  auto item = createAndInitDriveConfig(sourcedParameter);
  setConfigValue(item,sourcedParameter.value());
}

template<>
void DriveState::fillConfig<time_t>(cta::SourcedParameter<time_t>& sourcedParameter){
  auto item = createAndInitDriveConfig(sourcedParameter);
  setConfigValue(item,sourcedParameter.value());
}

//------------------------------------------------------------------------------
// DriveState::setConfig())
//------------------------------------------------------------------------------
void DriveState::setConfig(const cta::tape::daemon::TapedConfiguration& tapedConfiguration) {
  cta::tape::daemon::TapedConfiguration * config = const_cast<cta::tape::daemon::TapedConfiguration*>(&tapedConfiguration);
  
  m_payload.mutable_drive_config()->Clear();
  
  fillConfig(config->daemonUserName);
  fillConfig(config->daemonGroupName);
  fillConfig(config->logMask);
  fillConfig(config->tpConfigPath);
  fillConfig(config->bufferSizeBytes);
  fillConfig(config->bufferCount);
  fillConfig(config->archiveFetchBytesFiles);
  fillConfig(config->archiveFlushBytesFiles);
  fillConfig(config->retrieveFetchBytesFiles);
  fillConfig(config->mountCriteria);
  fillConfig(config->nbDiskThreads);
  fillConfig(config->useRAO);
  fillConfig(config->wdScheduleMaxSecs);
  fillConfig(config->wdMountMaxSecs);
  fillConfig(config->wdNoBlockMoveMaxSecs);
  fillConfig(config->wdIdleSessionTimer);
  fillConfig(config->backendPath);
  fillConfig(config->fileCatalogConfigFile);
  fillConfig(config->authenticationProtocol);
  fillConfig(config->authenticationSSSKeytab);
}

//------------------------------------------------------------------------------
// DriveState::setTpConfig())
//------------------------------------------------------------------------------
void DriveState::setTpConfig(const cta::tape::daemon::TpconfigLine& configLine){
  m_payload.set_dev_file_name(configLine.devFilename);
  m_payload.set_raw_library_slot(configLine.rawLibrarySlot);
}

//------------------------------------------------------------------------------
// DriveState::getDiskSpaceReservations())
//------------------------------------------------------------------------------
std::map<std::string, uint64_t> DriveState::getDiskSpaceReservations() {
  checkHeaderReadable();
  std::map<std::string, uint64_t>  ret;
  for (auto &dsr: m_payload.disk_space_reservations()) {
    ret[dsr.disk_system_name()] = dsr.reserved_bytes();
  }
  return ret;
}

//------------------------------------------------------------------------------
// DriveState::addDiskSpaceReservation())
//------------------------------------------------------------------------------
void DriveState::addDiskSpaceReservation(const std::string& diskSystemName, uint64_t bytes) {
  checkPayloadWritable();
  for (auto &dsr: *m_payload.mutable_disk_space_reservations()) {
    if (dsr.disk_system_name() == diskSystemName) {
      dsr.set_reserved_bytes(dsr.reserved_bytes() + bytes);
      return;
    }
  }
  auto * newDsr = m_payload.mutable_disk_space_reservations()->Add();
  newDsr->set_disk_system_name(diskSystemName);
  newDsr->set_reserved_bytes(bytes);
}

//------------------------------------------------------------------------------
// DriveState::substractDiskSpaceReservation())
//------------------------------------------------------------------------------
void DriveState::substractDiskSpaceReservation(const std::string& diskSystemName, uint64_t bytes) {
  checkPayloadWritable();
  size_t index=0;
  for (auto &dsr: *m_payload.mutable_disk_space_reservations()) {
    if (dsr.disk_system_name() == diskSystemName) {
      if (bytes > dsr.reserved_bytes())
        throw NegativeDiskSpaceReservationReached(
          "In DriveState::substractDiskSpaceReservation(): we would reach a negative reservation size.");
      dsr.set_reserved_bytes(dsr.reserved_bytes() - bytes);
      if (!dsr.reserved_bytes()) {
        // We can remove this entry from the list.
        auto * mdsr = m_payload.mutable_disk_space_reservations();
        mdsr->SwapElements(index, mdsr->size()-1);
        mdsr->RemoveLast();
      }
      return;
    } else {
      ++index;
    }
  }
  if (bytes) {
    std::stringstream err;
    err << "In DriveState::substractDiskSpaceReservation(): Trying to substract bytes without previous reservation. ";
    err << "dsr (";
    for (auto dsr: *m_payload.mutable_disk_space_reservations()) {
      err << "n:" << dsr.disk_system_name() << " rb:" << dsr.reserved_bytes();
    }
    err << ") b:" << bytes;
    throw NegativeDiskSpaceReservationReached(
        err.str()
      );
  }
}

//------------------------------------------------------------------------------
// DriveState::resetDiskSpaceReservation())
//------------------------------------------------------------------------------
void DriveState::resetDiskSpaceReservation() {
  checkPayloadWritable();
  m_payload.mutable_disk_space_reservations()->Clear();
}

//------------------------------------------------------------------------------
// DriveState::dump())
//------------------------------------------------------------------------------
std::string DriveState::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

//------------------------------------------------------------------------------
// DriveState::commit()
//------------------------------------------------------------------------------
void DriveState::commit(){
  checkPayloadWritable();
  m_payload.set_cta_version(CTA_VERSION);
  ObjectOps<serializers::DriveState, serializers::DriveState_t>::commit();
}

}} // namespace cta::objectstore
