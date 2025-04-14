// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/PhysicalLibrary.hpp"

namespace cta::frontend::grpc {

class PhysicalLibraryLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In PhysicalLibraryLsWriteReactor, we are inside OnWriteDone" << std::endl;
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
        std::list<cta::common::dataStructures::PhysicalLibrary> m_physicalLibraryList;
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::PhysicalLibrary>::const_iterator next_pl;
};

void PhysicalLibraryLsWriteReactor::OnDone() {
    std::cout << "In PhysicalLibraryLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

PhysicalLibraryLsWriteReactor::PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_physicalLibraryList(catalogue.PhysicalLibrary()->getPhysicalLibraries()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In PhysicalLibraryLsWriteReactor constructor, just entered!" << std::endl;

    next_pl = m_physicalLibraryList.cbegin();
    NextWrite();
}

void PhysicalLibraryLsWriteReactor::NextWrite() {
    std::cout << "In PhysicalLibraryLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::PHYSICALLIBRARY_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_pl != m_physicalLibraryList.cend()) {
            const auto& pl = *next_pl;
            next_pl++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::PhysicalLibraryLsItem *pl_item = data->mutable_plls_item();
            
            pl_item->set_name(pl.name);
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