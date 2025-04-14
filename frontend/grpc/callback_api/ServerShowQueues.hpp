// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/MountTypeSerDeser.hpp"

namespace cta::frontend::grpc {

class ShowQueuesWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        ShowQueuesWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::log::LogContext lc, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<cta::common::dataStructures::QueueAndMountSummary> m_queuesAndMountsList; 
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::QueueAndMountSummary>::const_iterator next_sq;
};

void ShowQueuesWriteReactor::OnDone() {
    delete this;
}

ShowQueuesWriteReactor::ShowQueuesWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::log::LogContext lc, const cta::xrd::Request* request)
    : m_queuesAndMountsList(scheduler.getQueuesAndMountSummaries(lc)),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In ShowQueuesWriteReactor constructor, just entered!" << std::endl;

    next_sq = m_queuesAndMountsList.cbegin();
    NextWrite();
}

void ShowQueuesWriteReactor::NextWrite() {
    m_response.Clear();
    using namespace cta::admin;
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::SHOWQUEUES);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_sq != m_queuesAndMountsList.cend()) {
            const auto& sq = *next_sq;
            next_sq++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::ShowQueuesItem *sq_item = data->mutable_sq_item();
            
            switch(sq.mountType) {
                case common::dataStructures::MountType::ArchiveForRepack:
                case common::dataStructures::MountType::ArchiveForUser:
                  sq_item->set_priority(sq.mountPolicy.archivePriority);
                  sq_item->set_min_age(sq.mountPolicy.archiveMinRequestAge);
                  break;
                case common::dataStructures::MountType::Retrieve:
                  sq_item->set_priority(sq.mountPolicy.retrievePriority);
                  sq_item->set_min_age(sq.mountPolicy.retrieveMinRequestAge);
                  break;
                default:
                  break;
            }
          
            sq_item->set_mount_type(MountTypeToProtobuf(sq.mountType));
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

            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end while
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc