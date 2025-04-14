// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"

namespace cta::frontend::grpc {

class VirtualOrganizationLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In VirtualOrganizationLsWriteReactor, we are inside OnWriteDone" << std::endl;
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<cta::common::dataStructures::VirtualOrganization> m_virtualOrganizationList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::VirtualOrganization>::const_iterator next_vo;
};

void VirtualOrganizationLsWriteReactor::OnDone() {
    std::cout << "In VirtualOrganizationLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

VirtualOrganizationLsWriteReactor::VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_virtualOrganizationList(catalogue.VO()->getVirtualOrganizations()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In VirtualOrganizationLsWriteReactor constructor, just entered!" << std::endl;

    next_vo = m_virtualOrganizationList.cbegin();
    NextWrite();
}

void VirtualOrganizationLsWriteReactor::NextWrite() {
    std::cout << "In VirtualOrganizationLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response(); // https://stackoverflow.com/questions/75693340/how-to-set-oneof-field-in-c-grpc-server-and-read-from-client
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::VIRTUALORGANIZATION_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_vo != m_virtualOrganizationList.cend()) {
            const auto& vo = *next_vo;
            next_vo++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::VirtualOrganizationLsItem *vo_item = data->mutable_vols_item();
            
            vo_item->set_name(vo.name);
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

            std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
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