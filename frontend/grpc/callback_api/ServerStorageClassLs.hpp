// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class StorageClassLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
    StorageClassLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
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
        std::list<common::dataStructures::StorageClass> m_storageClassList;
        bool m_isHeaderSent;
        std::optional<std::string> m_storageClassName;
        cta::xrd::StreamResponse m_response;
        std::list<common::dataStructures::StorageClass>::const_iterator next_sc;
};

void StorageClassLsWriteReactor::OnDone() {
    // add a log line here
    delete this;
}

// maybe also override the OnCancel method - Jacek implemented this

StorageClassLsWriteReactor::StorageClassLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
: m_isHeaderSent(false) {
    request::RequestMessage requestMsg(*request);
    m_storageClassName = requestMsg.getOptional(cta::admin::OptionString::STORAGE_CLASS);
    if(m_storageClassName.has_value()) {
        m_storageClassList.push_back(catalogue.StorageClass()->getStorageClass(m_storageClassName.value()));
    } else {
        for(const auto &storageClass : catalogue.StorageClass()->getStorageClasses()) {
            m_storageClassList.push_back(storageClass);
        }
    }
    next_sc = m_storageClassList.cbegin();
    NextWrite();
}

void StorageClassLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::STORAGECLASS_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_sc != m_storageClassList.cend()) {
            const auto& sc = *next_sc;
            ++next_sc;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::StorageClassLsItem *sc_item = data->mutable_scls_item();

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

            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end for
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc