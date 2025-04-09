// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class VirtualOrganizationLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::VirtualOrganization, cta::admin::VirtualOrganizationLsItem, decltype(&fillVirtualOrganizationItem)> {
    public:
        VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::VirtualOrganizationLsItem* getMessageField(cta::xrd::Data* data) { return data->mutable_vols_item(); }
};

VirtualOrganizationLsWriteReactor::VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::VirtualOrganization,
        cta::admin::VirtualOrganizationLsItem,
        decltype(&fillVirtualOrganizationItem)>
        (catalogue, scheduler, instanceName, catalogue.VO()->getVirtualOrganizations(), cta::admin::HeaderType::VIRTUALORGANIZATION_LS, &fillVirtualOrganizationItem) {}
} // namespace cta::frontend::grpc