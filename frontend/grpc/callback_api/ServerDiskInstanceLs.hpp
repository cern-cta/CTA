// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/DiskInstance.hpp"

namespace cta::frontend::grpc {

class DiskInstanceLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        DiskInstanceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In DiskInstanceLsWriteReactor, we are inside OnWriteDone" << std::endl;
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
        std::list<common::dataStructures::DiskInstance> m_diskInstanceList;  
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::list<common::dataStructures::DiskInstance>::const_iterator next_di;
};

void DiskInstanceLsWriteReactor::OnDone() {
    std::cout << "In DiskInstanceLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

DiskInstanceLsWriteReactor::DiskInstanceLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
: m_diskInstanceList(catalogue.DiskInstance()->getAllDiskInstances()),
  m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In DiskInstanceLsWriteReactor constructor, just entered!" << std::endl;

    next_di = m_diskInstanceList.cbegin();
    NextWrite();
}

void DiskInstanceLsWriteReactor::NextWrite() {
    std::cout << "In DiskInstanceLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response(); // https://stackoverflow.com/questions/75693340/how-to-set-oneof-field-in-c-grpc-server-and-read-from-client
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::DISKINSTANCE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        std::cout << "header was sent, now entering the loop to send the data, should send " << m_diskInstanceList.size() << " records!" << std::endl;
        while(next_di != m_diskInstanceList.cend()) {
            const auto& di = *next_di;
            ++next_di;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::DiskInstanceLsItem *di_item = data->mutable_dils_item();
            
            di_item->set_name(di.name);
            di_item->mutable_creation_log()->set_username(di.creationLog.username);
            di_item->mutable_creation_log()->set_host(di.creationLog.host);
            di_item->mutable_creation_log()->set_time(di.creationLog.time);
            di_item->mutable_last_modification_log()->set_username(di.lastModificationLog.username);
            di_item->mutable_last_modification_log()->set_host(di.lastModificationLog.host);
            di_item->mutable_last_modification_log()->set_time(di.lastModificationLog.time);
            di_item->set_comment(di.comment);

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