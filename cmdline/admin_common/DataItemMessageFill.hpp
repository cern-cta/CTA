#pragma once

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include <common/dataStructures/ArchiveRouteTypeSerDeser.hpp>
#include "common/dataStructures/QueueAndMountSummary.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"

inline void fillDiskInstanceItem(const cta::common::dataStructures::DiskInstance &di,
    cta::admin::DiskInstanceLsItem *di_item,
    const std::string m_instanceName) {
    di_item->set_name(di.name);
    di_item->set_instance_name(m_instanceName);
    di_item->mutable_creation_log()->set_username(di.creationLog.username);
    di_item->mutable_creation_log()->set_host(di.creationLog.host);
    di_item->mutable_creation_log()->set_time(di.creationLog.time);
    di_item->mutable_last_modification_log()->set_username(di.lastModificationLog.username);
    di_item->mutable_last_modification_log()->set_host(di.lastModificationLog.host);
    di_item->mutable_last_modification_log()->set_time(di.lastModificationLog.time);
    di_item->set_comment(di.comment);
}

inline void fillVirtualOrganizationItem(const cta::common::dataStructures::VirtualOrganization &vo,
    cta::admin::VirtualOrganizationLsItem *vo_item,
    const std::string& m_instanceName) {
    vo_item->set_name(vo.name);
    vo_item->set_instance_name(m_instanceName);
    vo_item->set_read_max_drives(vo.readMaxDrives);
    vo_item->set_write_max_drives(vo.writeMaxDrives);
    vo_item->set_max_file_size(vo.maxFileSize);
    vo_item->mutable_creation_log()->set_username(vo.creationLog.username);
    vo_item->mutable_creation_log()->set_host(vo.creationLog.host);
    vo_item->mutable_creation_log()->set_time(vo.creationLog.time);
    vo_item->mutable_last_modification_log()->set_username(vo.lastModificationLog.username);
    vo_item->mutable_last_modification_log()->set_host(vo.lastModificationLog.host);
    vo_item->mutable_last_modification_log()->set_time(vo.lastModificationLog.time);
    vo_item->set_comment(vo.comment);
    vo_item->set_diskinstance(vo.diskInstanceName);
    vo_item->set_is_repack_vo(vo.isRepackVo);
}

inline void fillArchiveRouteItem(const cta::common::dataStructures::ArchiveRoute &ar,
    cta::admin::ArchiveRouteLsItem *ar_item,
    const std::string& m_instanceName) {
    ar_item->set_storage_class(ar.storageClassName);
    ar_item->set_copy_number(ar.copyNb);
    ar_item->set_archive_route_type(cta::admin::ArchiveRouteTypeFormatToProtobuf(ar.type));
    ar_item->set_tapepool(ar.tapePoolName);
    ar_item->mutable_creation_log()->set_username(ar.creationLog.username);
    ar_item->mutable_creation_log()->set_host(ar.creationLog.host);
    ar_item->mutable_creation_log()->set_time(ar.creationLog.time);
    ar_item->mutable_last_modification_log()->set_username(ar.lastModificationLog.username);
    ar_item->mutable_last_modification_log()->set_host(ar.lastModificationLog.host);
    ar_item->mutable_last_modification_log()->set_time(ar.lastModificationLog.time);
    ar_item->set_comment(ar.comment);
    ar_item->set_instance_name(m_instanceName);
}

inline void fillSqItem(const cta::common::dataStructures::QueueAndMountSummary &sq,
    cta::admin::ShowQueuesItem *sq_item,
    std::optional<std::string> m_schedulerBackendName,
    const std::string m_instanceName) {

    switch(sq.mountType) {
        case cta::common::dataStructures::MountType::ArchiveForRepack:
        case cta::common::dataStructures::MountType::ArchiveForUser:
            sq_item->set_priority(sq.mountPolicy.archivePriority);
            sq_item->set_min_age(sq.mountPolicy.archiveMinRequestAge);
            break;
        case cta::common::dataStructures::MountType::Retrieve:
            sq_item->set_priority(sq.mountPolicy.retrievePriority);
            sq_item->set_min_age(sq.mountPolicy.retrieveMinRequestAge);
            break;
        default:
            break;
    }
  
    sq_item->set_mount_type(cta::admin::MountTypeToProtobuf(sq.mountType));
    sq_item->set_instance_name(m_instanceName);
    sq_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
    sq_item->set_tapepool(sq.tapePool);
    sq_item->set_logical_library(sq.logicalLibrary);
    sq_item->set_vid(sq.vid);
    sq_item->set_queued_files(sq.filesQueued);
    sq_item->set_queued_bytes(sq.bytesQueued);
    sq_item->set_oldest_age(sq.oldestJobAge);
    sq_item->set_youngest_age(sq.youngestJobAge);
    sq_item->set_cur_mounts(sq.currentMounts);
    sq_item->set_cur_files(sq.currentFiles);
    sq_item->set_cur_bytes(sq.currentBytes);
    sq_item->set_tapes_capacity(sq.tapesCapacity);
    sq_item->set_tapes_files(sq.filesOnTapes);
    sq_item->set_tapes_bytes(sq.dataOnTapes);
    sq_item->set_full_tapes(sq.fullTapes);
    sq_item->set_writable_tapes(sq.writableTapes);
    sq_item->set_vo(sq.vo);
    sq_item->set_read_max_drives(sq.readMaxDrives);
    sq_item->set_write_max_drives(sq.writeMaxDrives);
    if (sq.sleepForSpaceInfo) {
        sq_item->set_sleeping_for_space(true);
        sq_item->set_sleep_start_time(sq.sleepForSpaceInfo.value().startTime);
        sq_item->set_disk_system_slept_for(sq.sleepForSpaceInfo.value().diskSystemName);
    } else {
        sq_item->set_sleeping_for_space(false);
    }
    for (auto &policyName: sq.mountPolicies) {
        sq_item->add_mount_policies(policyName);
    }
    sq_item->set_highest_priority_mount_policy(sq.highestPriorityMountPolicy);
    sq_item->set_lowest_request_age_mount_policy(sq.lowestRequestAgeMountPolicy);
}

inline void fillTapeItem(const cta::common::dataStructures::Tape &tape,
    cta::admin::TapeLsItem *tape_item,
    const std::string m_instanceName) {

    tape_item->set_instance_name(m_instanceName);
    tape_item->set_vid(tape.vid);
    tape_item->set_media_type(tape.mediaType);
    tape_item->set_vendor(tape.vendor);
    tape_item->set_logical_library(tape.logicalLibraryName);
    tape_item->set_tapepool(tape.tapePoolName);
    tape_item->set_vo(tape.vo);
    tape_item->set_encryption_key_name(tape.encryptionKeyName.value_or(""));
    tape_item->set_capacity(tape.capacityInBytes);
    tape_item->set_occupancy(tape.dataOnTapeInBytes);
    tape_item->set_last_fseq(tape.lastFSeq);
    tape_item->set_full(tape.full);
    tape_item->set_dirty(tape.dirty);
    tape_item->set_from_castor(tape.isFromCastor);
    tape_item->set_label_format(cta::admin::LabelFormatToProtobuf(tape.labelFormat));
    tape_item->set_read_mount_count(tape.readMountCount);
    tape_item->set_write_mount_count(tape.writeMountCount);
    tape_item->set_nb_master_files(tape.nbMasterFiles);
    tape_item->set_master_data_in_bytes(tape.masterDataInBytes);
    
    if(tape.labelLog) {
        ::cta::common::TapeLog * labelLog = tape_item->mutable_label_log();
        labelLog->set_drive(tape.labelLog.value().drive);
        labelLog->set_time(tape.labelLog.value().time);
    }
    if(tape.lastWriteLog){
        ::cta::common::TapeLog * lastWriteLog = tape_item->mutable_last_written_log();
        lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
        lastWriteLog->set_time(tape.lastWriteLog.value().time);
    }
    if(tape.lastReadLog){
        ::cta::common::TapeLog * lastReadLog = tape_item->mutable_last_read_log();
        lastReadLog->set_drive(tape.lastReadLog.value().drive);
        lastReadLog->set_time(tape.lastReadLog.value().time);
    }
    ::cta::common::EntryLog * creationLog = tape_item->mutable_creation_log();
    creationLog->set_username(tape.creationLog.username);
    creationLog->set_host(tape.creationLog.host);
    creationLog->set_time(tape.creationLog.time);
    ::cta::common::EntryLog * lastModificationLog = tape_item->mutable_last_modification_log();
    lastModificationLog->set_username(tape.lastModificationLog.username);
    lastModificationLog->set_host(tape.lastModificationLog.host);
    lastModificationLog->set_time(tape.lastModificationLog.time);
    tape_item->set_comment(tape.comment);

    tape_item->set_state(tape.getStateStr());
    tape_item->set_state_reason(tape.stateReason ? tape.stateReason.value() : "");
    tape_item->set_purchase_order(tape.purchaseOrder ? tape.purchaseOrder.value() : "");
    tape_item->set_physical_library(tape.physicalLibraryName ? tape.physicalLibraryName.value() : "");
    tape_item->set_state_update_time(tape.stateUpdateTime);
    tape_item->set_state_modified_by(tape.stateModifiedBy);
    if (tape.verificationStatus) {
        tape_item->set_verification_status(tape.verificationStatus.value());
    }
}

inline void fillActivityMountRuleItem(const cta::common::dataStructures::RequesterActivityMountRule &amr,
    cta::admin::ActivityMountRuleLsItem *amr_item,
    const std::string m_instanceName) {
    amr_item->set_disk_instance(amr.diskInstance);
    amr_item->set_activity_mount_rule(amr.name);
    amr_item->set_mount_policy(amr.mountPolicy);
    amr_item->set_activity_regex(amr.activityRegex);
    amr_item->mutable_creation_log()->set_username(amr.creationLog.username);
    amr_item->mutable_creation_log()->set_host(amr.creationLog.host);
    amr_item->mutable_creation_log()->set_time(amr.creationLog.time);
    amr_item->mutable_last_modification_log()->set_username(amr.lastModificationLog.username);
    amr_item->mutable_last_modification_log()->set_host(amr.lastModificationLog.host);
    amr_item->mutable_last_modification_log()->set_time(amr.lastModificationLog.time);
    amr_item->set_comment(amr.comment),
    amr_item->set_instance_name(m_instanceName);
}

inline void fillGroupMountRuleItem(const cta::common::dataStructures::RequesterGroupMountRule &gmr,
    cta::admin::GroupMountRuleLsItem *gmr_item,
    const std::string m_instanceName) {
    gmr_item->set_instance_name(m_instanceName);
    gmr_item->set_disk_instance(gmr.diskInstance);
    gmr_item->set_group_mount_rule(gmr.name);
    gmr_item->set_mount_policy(gmr.mountPolicy);
    gmr_item->mutable_creation_log()->set_username(gmr.creationLog.username);
    gmr_item->mutable_creation_log()->set_host(gmr.creationLog.host);
    gmr_item->mutable_creation_log()->set_time(gmr.creationLog.time);
    gmr_item->mutable_last_modification_log()->set_username(gmr.lastModificationLog.username);
    gmr_item->mutable_last_modification_log()->set_host(gmr.lastModificationLog.host);
    gmr_item->mutable_last_modification_log()->set_time(gmr.lastModificationLog.time);
    gmr_item->set_comment(gmr.comment);
}

inline void fillRepackRequestItem(const cta::common::dataStructures::RepackInfo &repackRequest,
    cta::admin::RepackLsItem *repackRequestItem,
    cta::common::dataStructures::VidToTapeMap tapeVidMap,
    const std::string& m_instanceName) {
        uint64_t filesLeftToRetrieve = repackRequest.totalFilesToRetrieve - repackRequest.retrievedFiles;
        uint64_t filesLeftToArchive = repackRequest.totalFilesToArchive - repackRequest.archivedFiles;
        uint64_t totalFilesToRetrieve = repackRequest.totalFilesToRetrieve;
        uint64_t totalFilesToArchive = repackRequest.totalFilesToArchive;
        
        repackRequestItem->set_instance_name(m_instanceName);
        repackRequestItem->set_vid(repackRequest.vid);
        repackRequestItem->set_tapepool(tapeVidMap[repackRequest.vid].tapePoolName);
        repackRequestItem->set_repack_buffer_url(repackRequest.repackBufferBaseURL);
        repackRequestItem->set_user_provided_files(repackRequest.userProvidedFiles);
        repackRequestItem->set_total_files_on_tape_at_start(repackRequest.totalFilesOnTapeAtStart);
        repackRequestItem->set_total_bytes_on_tape_at_start(repackRequest.totalBytesOnTapeAtStart);
        repackRequestItem->set_all_files_selected_at_start(repackRequest.allFilesSelectedAtStart);
        repackRequestItem->set_total_files_to_retrieve(totalFilesToRetrieve);
        repackRequestItem->set_total_bytes_to_retrieve(repackRequest.totalBytesToRetrieve);
        repackRequestItem->set_total_files_to_archive(totalFilesToArchive);
        repackRequestItem->set_total_bytes_to_archive(repackRequest.totalBytesToArchive);
        repackRequestItem->set_retrieved_files(repackRequest.retrievedFiles);
        repackRequestItem->set_retrieved_bytes(repackRequest.retrievedBytes);
        repackRequestItem->set_files_left_to_retrieve(filesLeftToRetrieve);
        repackRequestItem->set_files_left_to_archive(filesLeftToArchive);
        repackRequestItem->set_archived_files(repackRequest.archivedFiles);
        repackRequestItem->set_archived_bytes(repackRequest.archivedBytes);
        repackRequestItem->set_failed_to_retrieve_files(repackRequest.failedFilesToRetrieve);
        repackRequestItem->set_failed_to_retrieve_bytes(repackRequest.failedBytesToRetrieve);
        repackRequestItem->set_failed_to_archive_files(repackRequest.failedFilesToArchive);
        repackRequestItem->set_failed_to_archive_bytes(repackRequest.failedBytesToArchive);
        repackRequestItem->set_total_failed_files(repackRequest.failedFilesToRetrieve + repackRequest.failedFilesToArchive);
        repackRequestItem->set_status(toString(repackRequest.status));
        uint64_t repackTime = time(nullptr) - repackRequest.creationLog.time;
          repackRequestItem->set_repack_finished_time(repackRequest.repackFinishedTime);
        if(repackRequest.repackFinishedTime != 0){
          //repackFinishedTime != 0: repack is finished
          repackTime = repackRequest.repackFinishedTime - repackRequest.creationLog.time;
        }
        repackRequestItem->set_repack_time(repackTime);
        repackRequestItem->mutable_creation_log()->set_username(repackRequest.creationLog.username);
        repackRequestItem->mutable_creation_log()->set_host(repackRequest.creationLog.host);
        repackRequestItem->mutable_creation_log()->set_time(repackRequest.creationLog.time);
        //Last expanded fSeq is in reality the next FSeq to Expand. So last one is next - 1
        repackRequestItem->set_last_expanded_fseq(repackRequest.lastExpandedFseq != 0 ? repackRequest.lastExpandedFseq - 1 : 0);
        repackRequestItem->mutable_destination_infos()->Clear();
        for(auto destinationInfo: repackRequest.destinationInfos){
          auto * destinationInfoToInsert = repackRequestItem->mutable_destination_infos()->Add();
          destinationInfoToInsert->set_vid(destinationInfo.vid);
          destinationInfoToInsert->set_files(destinationInfo.files);
          destinationInfoToInsert->set_bytes(destinationInfo.bytes);
        }
}

inline void fillLogicalLibraryItem(const cta::common::dataStructures::LogicalLibrary &ll,
    cta::admin::LogicalLibraryLsItem *ll_item,
    const std::string& m_instanceName) {
    
        ll_item->set_name(ll.name);
        ll_item->set_is_disabled(ll.isDisabled);
        if(ll.physicalLibraryName) {
        ll_item->set_physical_library(ll.physicalLibraryName.value());
        }
        if (ll.disabledReason) {
        ll_item->set_disabled_reason(ll.disabledReason.value());
        }
        ll_item->mutable_creation_log()->set_username(ll.creationLog.username);
        ll_item->mutable_creation_log()->set_host(ll.creationLog.host);
        ll_item->mutable_creation_log()->set_time(ll.creationLog.time);
        ll_item->mutable_last_modification_log()->set_username(ll.lastModificationLog.username);
        ll_item->mutable_last_modification_log()->set_host(ll.lastModificationLog.host);
        ll_item->mutable_last_modification_log()->set_time(ll.lastModificationLog.time);
        ll_item->set_comment(ll.comment);
        ll_item->set_instance_name(m_instanceName);
}

inline void fillPhysicalLibraryItem(const cta::common::dataStructures::PhysicalLibrary &pl,
    cta::admin::PhysicalLibraryLsItem *pl_item,
    const std::string& m_instanceName) {
    pl_item->set_name(pl.name);
    pl_item->set_instance_name(m_instanceName);
    pl_item->set_manufacturer(pl.manufacturer);
    pl_item->set_model(pl.model);

    if (pl.type)                      { pl_item->set_type(pl.type.value()); }
    if (pl.guiUrl)                    { pl_item->set_gui_url(pl.guiUrl.value()); }
    if (pl.webcamUrl)                 { pl_item->set_webcam_url(pl.webcamUrl.value()); }
    if (pl.location)                  { pl_item->set_location(pl.location.value()); }
    if (pl.nbAvailableCartridgeSlots) { pl_item->set_nb_available_cartridge_slots(pl.nbAvailableCartridgeSlots.value()); }
    if (pl.comment)                   { pl_item->set_comment(pl.comment.value()); }
    if (pl.disabledReason) {
      pl_item->set_disabled_reason(pl.disabledReason.value());
    }

    pl_item->set_nb_physical_cartridge_slots(pl.nbPhysicalCartridgeSlots);
    pl_item->set_nb_physical_drive_slots(pl.nbPhysicalDriveSlots);
    pl_item->mutable_creation_log()->set_username(pl.creationLog.username);
    pl_item->mutable_creation_log()->set_host(pl.creationLog.host);
    pl_item->mutable_creation_log()->set_time(pl.creationLog.time);
    pl_item->mutable_last_modification_log()->set_username(pl.lastModificationLog.username);
    pl_item->mutable_last_modification_log()->set_host(pl.lastModificationLog.host);
    pl_item->mutable_last_modification_log()->set_time(pl.lastModificationLog.time);
    pl_item->set_is_disabled(pl.isDisabled);
}

inline void fillDriveItem(const cta::common::dataStructures::TapeDrive &dr,
    cta::admin::DriveLsItem *dr_item,
    const std::string m_instanceName,
    std::string driveSchedulerBackendName,
    std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> driveConfigs) {
        dr_item->set_instance_name(m_instanceName);
        dr_item->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
        dr_item->set_logical_library(dr.logicalLibrary);
        dr_item->set_drive_name(dr.driveName);
        dr_item->set_scheduler_backend_name(driveSchedulerBackendName);
        dr_item->set_host(dr.host);
        dr_item->set_logical_library_disabled(dr.logicalLibraryDisabled ? dr.logicalLibraryDisabled.value() : false);
        dr_item->set_desired_drive_state(dr.desiredUp ? cta::admin::DriveLsItem::UP : cta::admin::DriveLsItem::DOWN);
        dr_item->set_mount_type(cta::admin::MountTypeToProtobuf(dr.mountType));
        dr_item->set_drive_status(cta::admin::DriveStatusToProtobuf(dr.driveStatus));
        dr_item->set_vid(dr.currentVid ? dr.currentVid.value() : "");
        dr_item->set_tapepool(dr.currentTapePool ? dr.currentTapePool.value() : "");
        dr_item->set_vo(dr.currentVo ? dr.currentVo.value() : "");
        dr_item->set_files_transferred_in_session(dr.filesTransferedInSession ? dr.filesTransferedInSession.value() : 0);
        dr_item->set_bytes_transferred_in_session(dr.bytesTransferedInSession ? dr.bytesTransferedInSession.value() : 0);
        dr_item->set_session_id(dr.sessionId ? dr.sessionId.value() : 0);
        const auto lastUpdateTime = dr.lastModificationLog ? dr.lastModificationLog.value().time : time(nullptr);
        dr_item->set_time_since_last_update(time(nullptr) - lastUpdateTime);
        dr_item->set_current_priority(dr.currentPriority ? dr.currentPriority.value() : 0);
        dr_item->set_current_activity(dr.currentActivity ? dr.currentActivity.value() : "");
        dr_item->set_dev_file_name(dr.devFileName ? dr.devFileName.value() : "");
        dr_item->set_raw_library_slot(dr.rawLibrarySlot ? dr.rawLibrarySlot.value() : "");
        dr_item->set_comment(dr.userComment ? dr.userComment.value() : "");
        dr_item->set_reason(dr.reasonUpDown ? dr.reasonUpDown.value() : "");
        dr_item->set_physical_library(dr.physicalLibraryName ? dr.physicalLibraryName.value() : "");
        dr_item->set_physical_library_disabled(dr.physicalLibraryDisabled ? dr.physicalLibraryDisabled.value() : false);
        if (dr.mountType == cta::common::dataStructures::MountType::Retrieve) {
        dr_item->set_disk_system_name(dr.diskSystemName ? dr.diskSystemName.value() : "");
        dr_item->set_reserved_bytes(dr.reservedBytes ? dr.reservedBytes.value() : 0);
        }
        dr_item->set_session_elapsed_time(dr.sessionElapsedTime ? dr.sessionElapsedTime.value() : 0);

        auto driveConfig = dr_item->mutable_drive_config();

        for (const auto& config : driveConfigs) {
        auto driveConfigItemProto = driveConfig->Add();
        driveConfigItemProto->set_key(config.keyName);
        driveConfigItemProto->set_category(config.category);
        driveConfigItemProto->set_value(config.value);
        driveConfigItemProto->set_source(config.source);
        }

        // set the time spent in the current state
        uint64_t drive_time = time(nullptr);

        switch (dr.driveStatus) {
        using cta::common::dataStructures::DriveStatus;
            // clang-format off
        case DriveStatus::Probing:           drive_time -= dr.probeStartTime.value();    break;
        case DriveStatus::Up:                drive_time -= dr.downOrUpStartTime.value(); break;
        case DriveStatus::Down:              drive_time -= dr.downOrUpStartTime.value(); break;
        case DriveStatus::Starting:          drive_time -= dr.startStartTime.value();    break;
        case DriveStatus::Mounting:          drive_time -= dr.mountStartTime.value();    break;
        case DriveStatus::Transferring:      drive_time -= dr.transferStartTime.value(); break;
        case DriveStatus::CleaningUp:        drive_time -= dr.cleanupStartTime.value();  break;
        case DriveStatus::Unloading:         drive_time -= dr.unloadStartTime.value();   break;
        case DriveStatus::Unmounting:        drive_time -= dr.unmountStartTime.value();  break;
        case DriveStatus::DrainingToDisk:    drive_time -= dr.drainingStartTime.value(); break;
        case DriveStatus::Shutdown:          drive_time -= dr.shutdownTime.value();      break;
        case DriveStatus::Unknown:                                                       break;
            // clang-format on
        }
        dr_item->set_drive_status_since(drive_time);
    }