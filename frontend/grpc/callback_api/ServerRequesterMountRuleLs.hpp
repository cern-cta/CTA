// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class RequesterMountRuleLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::RequesterMountRule, cta::admin::RequesterMountRuleLsItem, decltype(&fillRequesterMountRuleItem)> {
    public:
        cta::admin::RequesterMountRuleLsItem* getMessageField(cta::xrd::Data* data) override;
        RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

RequesterMountRuleLsWriteReactor::RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::RequesterMountRule,
      cta::admin::RequesterMountRuleLsItem,
      decltype(&fillRequesterMountRuleItem)>
      (catalogue, scheduler, instanceName, catalogue.RequesterMountRule()->getRequesterMountRules(), cta::admin::HeaderType::GROUPMOUNTRULE_LS, &fillRequesterMountRuleItem) {}

cta::admin::RequesterMountRuleLsItem* RequesterMountRuleLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_rmrls_item();
}

} // namespace cta::frontend::grpc