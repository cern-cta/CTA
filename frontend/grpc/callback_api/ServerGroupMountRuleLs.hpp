// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class GroupMountRuleLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::RequesterGroupMountRule, cta::admin::GroupMountRuleLsItem, decltype(&fillGroupMountRuleItem)> {
    public:
        GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::GroupMountRuleLsItem* getMessageField(cta::xrd::Data* data) override;
};

GroupMountRuleLsWriteReactor::GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const std::string& instanceName,
                                                           const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::RequesterGroupMountRule,
                             cta::admin::GroupMountRuleLsItem,
                             decltype(&fillGroupMountRuleItem)>(
        catalogue,
        scheduler,
        instanceName,
        catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules(),
        cta::admin::HeaderType::GROUPMOUNTRULE_LS,
        &fillGroupMountRuleItem) {}

cta::admin::GroupMountRuleLsItem* GroupMountRuleLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_gmrls_item();
}
} // namespace cta::frontend::grpc