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
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "disk/DiskSystem.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "catalogue/TapePool.hpp"

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
    const std::string& m_instanceName,
    cta::common::dataStructures::VidToTapeMap tapeVidMap) {
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

inline void fillAdminItem(const cta::common::dataStructures::AdminUser &ad,
    cta::admin::AdminLsItem *ad_item,
    const std::string& m_instanceName) {
        ad_item->set_user(ad.name);
        ad_item->mutable_creation_log()->set_username(ad.creationLog.username);
        ad_item->mutable_creation_log()->set_host(ad.creationLog.host);
        ad_item->mutable_creation_log()->set_time(ad.creationLog.time);
        ad_item->mutable_last_modification_log()->set_username(ad.lastModificationLog.username);
        ad_item->mutable_last_modification_log()->set_host(ad.lastModificationLog.host);
        ad_item->mutable_last_modification_log()->set_time(ad.lastModificationLog.time);
        ad_item->set_comment(ad.comment);
        ad_item->set_instance_name(m_instanceName);
    }

inline void fillDiskInstanceSpaceItem(const cta::common::dataStructures::DiskInstanceSpace &dis,
    cta::admin::DiskInstanceSpaceLsItem *dis_item,
    const std::string& m_instanceName) {
        dis_item->set_name(dis.name);
        dis_item->set_instance_name(dis.name);
        dis_item->set_disk_instance(dis.diskInstance);
        dis_item->set_refresh_interval(dis.refreshInterval);
        dis_item->set_free_space_query_url(dis.freeSpaceQueryURL);
        dis_item->set_free_space(dis.freeSpace);
        dis_item->mutable_creation_log()->set_username(dis.creationLog.username);
        dis_item->mutable_creation_log()->set_host(dis.creationLog.host);
        dis_item->mutable_creation_log()->set_time(dis.creationLog.time);
        dis_item->mutable_last_modification_log()->set_username(dis.lastModificationLog.username);
        dis_item->mutable_last_modification_log()->set_host(dis.lastModificationLog.host);
        dis_item->mutable_last_modification_log()->set_time(dis.lastModificationLog.time);
        dis_item->set_comment(dis.comment);
    }

inline void fillDiskSystemItem(const cta::disk::DiskSystem &ds,
    cta::admin::DiskSystemLsItem *ds_item,
    const std::string& m_instanceName) {
        ds_item->set_name(ds.name);
        ds_item->set_instance_name(m_instanceName);
        ds_item->set_file_regexp(ds.fileRegexp);
        ds_item->set_disk_instance(ds.diskInstanceSpace.diskInstance);
        ds_item->set_disk_instance_space(ds.diskInstanceSpace.name);
        ds_item->set_targeted_free_space(ds.targetedFreeSpace);
        ds_item->set_sleep_time(ds.sleepTime);
        ds_item->mutable_creation_log()->set_username(ds.creationLog.username);
        ds_item->mutable_creation_log()->set_host(ds.creationLog.host);
        ds_item->mutable_creation_log()->set_time(ds.creationLog.time);
        ds_item->mutable_last_modification_log()->set_username(ds.lastModificationLog.username);
        ds_item->mutable_last_modification_log()->set_host(ds.lastModificationLog.host);
        ds_item->mutable_last_modification_log()->set_time(ds.lastModificationLog.time);
        ds_item->set_comment(ds.comment);
    }

inline void fillRecycleTapeFileItem(const cta::common::dataStructures::FileRecycleLog &fileRecycleLog,
    cta::admin::RecycleTapeFileLsItem *recycleLogToReturn,
    const std::string& m_instanceName) {
        recycleLogToReturn->set_vid(fileRecycleLog.vid);
        recycleLogToReturn->set_fseq(fileRecycleLog.fSeq);
        recycleLogToReturn->set_block_id(fileRecycleLog.blockId);
        recycleLogToReturn->set_copy_nb(fileRecycleLog.copyNb);
        recycleLogToReturn->set_tape_file_creation_time(fileRecycleLog.tapeFileCreationTime);
        recycleLogToReturn->set_archive_file_id(fileRecycleLog.archiveFileId);
        recycleLogToReturn->set_disk_instance(fileRecycleLog.diskInstanceName);
        recycleLogToReturn->set_disk_file_id(fileRecycleLog.diskFileId);
        recycleLogToReturn->set_disk_file_id_when_deleted(fileRecycleLog.diskFileIdWhenDeleted);
        recycleLogToReturn->set_disk_file_uid(fileRecycleLog.diskFileUid);
        recycleLogToReturn->set_disk_file_gid(fileRecycleLog.diskFileGid);
        recycleLogToReturn->set_size_in_bytes(fileRecycleLog.sizeInBytes);
        
        // Checksum
        cta::common::ChecksumBlob csb;
        cta::checksum::ChecksumBlobToProtobuf(fileRecycleLog.checksumBlob, csb);
        for(auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
          auto cs_ptr = recycleLogToReturn->add_checksum();
          cs_ptr->set_type(csb_it->type());
          cs_ptr->set_value(cta::checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
        }
        recycleLogToReturn->set_storage_class(fileRecycleLog.storageClassName);
        recycleLogToReturn->set_virtual_organization(fileRecycleLog.vo);
        recycleLogToReturn->set_archive_file_creation_time(fileRecycleLog.archiveFileCreationTime);
        recycleLogToReturn->set_reconciliation_time(fileRecycleLog.reconciliationTime);
        if(fileRecycleLog.collocationHint){
          recycleLogToReturn->set_collocation_hint(fileRecycleLog.collocationHint.value());
        }
        if(fileRecycleLog.diskFilePath){
          recycleLogToReturn->set_disk_file_path(fileRecycleLog.diskFilePath.value());
        }
        recycleLogToReturn->set_reason_log(fileRecycleLog.reasonLog);
        recycleLogToReturn->set_recycle_log_time(fileRecycleLog.recycleLogTime);
}

inline void fillRequesterMountRuleItem(const cta::common::dataStructures::RequesterMountRule &rmr,
    cta::admin::RequesterMountRuleLsItem *rmr_item,
    const std::string& m_instanceName) {
        rmr_item->set_instance_name(m_instanceName);
        rmr_item->set_disk_instance(rmr.diskInstance);
        rmr_item->set_requester_mount_rule(rmr.name);
        rmr_item->set_mount_policy(rmr.mountPolicy);
        rmr_item->mutable_creation_log()->set_username(rmr.creationLog.username);
        rmr_item->mutable_creation_log()->set_host(rmr.creationLog.host);
        rmr_item->mutable_creation_log()->set_time(rmr.creationLog.time);
        rmr_item->mutable_last_modification_log()->set_username(rmr.lastModificationLog.username);
        rmr_item->mutable_last_modification_log()->set_host(rmr.lastModificationLog.host);
        rmr_item->mutable_last_modification_log()->set_time(rmr.lastModificationLog.time);
        rmr_item->set_comment(rmr.comment);
}

inline void fillStorageClassItem(const cta::common::dataStructures::StorageClass &sc,
    cta::admin::StorageClassLsItem *sc_item,
    const std::string& m_instanceName) {
        sc_item->set_name(sc.name);
        sc_item->set_nb_copies(sc.nbCopies);
        sc_item->set_vo(sc.vo.name);
        sc_item->mutable_creation_log()->set_username(sc.creationLog.username);
        sc_item->mutable_creation_log()->set_host(sc.creationLog.host);
        sc_item->mutable_creation_log()->set_time(sc.creationLog.time);
        sc_item->mutable_last_modification_log()->set_username(sc.lastModificationLog.username);
        sc_item->mutable_last_modification_log()->set_host(sc.lastModificationLog.host);
        sc_item->mutable_last_modification_log()->set_time(sc.lastModificationLog.time);
        sc_item->set_comment(sc.comment);
        sc_item->set_instance_name(m_instanceName);
}

inline void fillTapePoolItem(const cta::catalogue::TapePool& tp,
    cta::admin::TapePoolLsItem* tp_item,
    const std::string& instanceName) {
        tp_item->set_name(tp.name);
        tp_item->set_instance_name(instanceName);
        tp_item->set_vo(tp.vo.name);
        tp_item->set_num_tapes(tp.nbTapes);
        tp_item->set_num_partial_tapes(tp.nbPartialTapes);
        tp_item->set_num_physical_files(tp.nbPhysicalFiles);
        tp_item->set_capacity_bytes(tp.capacityBytes);
        tp_item->set_data_bytes(tp.dataBytes);
        tp_item->set_encrypt(tp.encryption);
        tp_item->set_encryption_key_name(tp.encryptionKeyName.value_or(""));
        tp_item->set_supply(tp.supply ? tp.supply.value() : "");
        tp_item->mutable_created()->set_username(tp.creationLog.username);
        tp_item->mutable_created()->set_host(tp.creationLog.host);
        tp_item->mutable_created()->set_time(tp.creationLog.time);
        tp_item->mutable_modified()->set_username(tp.lastModificationLog.username);
        tp_item->mutable_modified()->set_host(tp.lastModificationLog.host);
        tp_item->mutable_modified()->set_time(tp.lastModificationLog.time);
        tp_item->set_comment(tp.comment);
        for (auto& source : tp.supply_source_set) {
            tp_item->add_supply_source(source);
        }
        for (auto& destination : tp.supply_destination_set) {
            tp_item->add_supply_destination(destination);
        }
}

inline void fillArchiveJobFailedRequestItem(const cta::common::dataStructures::ArchiveJob& item,
    cta::admin::FailedRequestLsItem* fr_item,
    const std::string& instanceName,
    const std::string& schedulerBackendName,
    const std::string& tapepool,
    bool isLogEntries) {
        fr_item->set_object_id(item.objectId);
        fr_item->set_request_type(cta::admin::RequestType::ARCHIVE_REQUEST);
        fr_item->set_tapepool(tapepool);
        fr_item->set_copy_nb(item.copyNumber);
        fr_item->mutable_requester()->set_username(item.request.requester.name);
        fr_item->mutable_requester()->set_groupname(item.request.requester.group);
        fr_item->mutable_af()->set_archive_id(item.archiveFileID);
        fr_item->mutable_af()->set_disk_instance(item.instanceName);
        fr_item->mutable_af()->set_disk_id(item.request.diskFileID);
        fr_item->mutable_af()->set_size(item.request.fileSize);
        fr_item->mutable_af()->set_storage_class(item.request.storageClass);
        fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
        fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
        fr_item->set_totalretries(item.totalRetries);
        fr_item->set_totalreportretries(item.totalReportRetries);
        if (isLogEntries) {
            *fr_item->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
            *fr_item->mutable_reportfailurelogs() = { item.reportfailurelogs.begin(), item.reportfailurelogs.end() };
        }
        fr_item->set_scheduler_backend_name(schedulerBackendName);
        fr_item->set_instance_name(instanceName);
    }

inline void fillRetrieveJobFailedRequestItem(const cta::common::dataStructures::RetrieveJob& item,
    cta::admin::FailedRequestLsItem* fr_item,
    const std::string& instanceName,
    const std::string& schedulerBackendName, // maybe should be optional
    const std::string& vid,
    bool isLogEntries) {
        fr_item->set_object_id(item.objectId);
        fr_item->set_request_type(cta::admin::RequestType::RETRIEVE_REQUEST);
        fr_item->set_copy_nb(item.tapeCopies.at(vid).first);
        fr_item->mutable_requester()->set_username(item.request.requester.name);
        fr_item->mutable_requester()->set_groupname(item.request.requester.group);
        fr_item->mutable_af()->set_archive_id(item.request.archiveFileID);
        fr_item->mutable_af()->set_size(item.fileSize);
        fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
        fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
        fr_item->mutable_tf()->set_vid(vid);
        fr_item->set_totalretries(item.totalRetries);
        fr_item->set_totalreportretries(item.totalReportRetries);

        // Find the correct tape copy
        for (auto &tapecopy : item.tapeCopies) {
            auto &tf = tapecopy.second.second;
            if (tf.vid == vid) {
            fr_item->mutable_tf()->set_f_seq(tf.fSeq);
            fr_item->mutable_tf()->set_block_id(tf.blockId);
            break;
            }
        }

        if (isLogEntries) {
            *fr_item->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
            *fr_item->mutable_reportfailurelogs() = {item.reportfailurelogs.begin(), item.reportfailurelogs.end()};
        }
        fr_item->set_scheduler_backend_name(schedulerBackendName);
        fr_item->set_instance_name(instanceName);
    }