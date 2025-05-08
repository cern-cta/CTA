#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/dataStructures/QueueAndMountSummary.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"

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