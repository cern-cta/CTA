// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class VirtualOrganizationLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::VirtualOrganization> m_virtualOrganizationList; 
        std::list<cta::common::dataStructures::VirtualOrganization>::const_iterator next_vo;
};

VirtualOrganizationLsWriteReactor::VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_virtualOrganizationList(catalogue.VO()->getVirtualOrganizations()) {
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
            ++next_vo;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::VirtualOrganizationLsItem *vo_item = data->mutable_vols_item();
            
            fillVirtualOrganizationItem(vo, vo_item, m_instanceName);

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