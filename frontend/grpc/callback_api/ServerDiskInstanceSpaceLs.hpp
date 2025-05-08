// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class DiskInstanceSpaceLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaceList;
        std::list<common::dataStructures::DiskInstanceSpace>::const_iterator next_dis;
};


DiskInstanceSpaceLsWriteReactor::DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_diskInstanceSpaceList(catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces()) {

    next_dis = m_diskInstanceSpaceList.cbegin();
    NextWrite();
}

void DiskInstanceSpaceLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::DISKINSTANCESPACE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_dis != m_diskInstanceSpaceList.cend()) {
            const auto& dis = *next_dis;
            ++next_dis;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::DiskInstanceSpaceLsItem *dis_item = data->mutable_disls_item();

            dis_item->set_name(dis.name);
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