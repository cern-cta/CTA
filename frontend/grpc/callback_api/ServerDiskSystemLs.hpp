// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "disk/DiskSystem.hpp"

namespace cta::frontend::grpc {

class DiskSystemLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        DiskSystemLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
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
        cta::disk::DiskSystemList m_diskSystemList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<disk::DiskSystem>::const_iterator next_ds;
};

void DiskSystemLsWriteReactor::OnDone() {
    delete this;
}

DiskSystemLsWriteReactor::DiskSystemLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_diskSystemList(catalogue.DiskSystem()->getAllDiskSystems()),
      m_isHeaderSent(false) {

    next_ds = m_diskSystemList.cbegin();
    NextWrite();
}

void DiskSystemLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::DISKSYSTEM_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_ds != m_diskSystemList.cend()) {
            const auto& ds = *next_ds;
            ++next_ds;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::DiskSystemLsItem *ds_item = data->mutable_dsls_item();

            ds_item->set_name(ds.name);
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