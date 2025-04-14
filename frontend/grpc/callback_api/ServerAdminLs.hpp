// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/AdminUser.hpp"

namespace cta::frontend::grpc {

class AdminLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        AdminLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In AdminLsWriteReactor, we are inside OnWriteDone" << std::endl;
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
        std::list<cta::common::dataStructures::AdminUser> m_adminList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::AdminUser>::const_iterator next_admin;
};

void AdminLsWriteReactor::OnDone() {
    std::cout << "In AdminLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

AdminLsWriteReactor::AdminLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_adminList(catalogue.AdminUser()->getAdminUsers()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In AdminLsWriteReactor constructor, just entered!" << std::endl;

    next_admin = m_adminList.cbegin();
    NextWrite();
}

void AdminLsWriteReactor::NextWrite() {
    std::cout << "In AdminLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::ADMIN_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_admin != m_adminList.cend()) {
            const auto& ad = *next_admin;
            next_admin++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::AdminLsItem *ad_item = data->mutable_adls_item();
            
            ad_item->set_user(ad.name);
            ad_item->mutable_creation_log()->set_username(ad.creationLog.username);
            ad_item->mutable_creation_log()->set_host(ad.creationLog.host);
            ad_item->mutable_creation_log()->set_time(ad.creationLog.time);
            ad_item->mutable_last_modification_log()->set_username(ad.lastModificationLog.username);
            ad_item->mutable_last_modification_log()->set_host(ad.lastModificationLog.host);
            ad_item->mutable_last_modification_log()->set_time(ad.lastModificationLog.time);
            ad_item->set_comment(ad.comment);

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