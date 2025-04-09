#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include "TemplateAdminCmdStream.hpp"
namespace cta::frontend::grpc {

class ActivityMountRuleLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::RequesterActivityMountRule, cta::admin::ActivityMountRuleLsItem, decltype(&fillActivityMountRuleItem)> {
    public:
        ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::ActivityMountRuleLsItem* getMessageField(cta::xrd::Data *data) override;
};

ActivityMountRuleLsWriteReactor::ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                                 cta::Scheduler& scheduler,
                                                                 const std::string& instanceName,
                                                                 const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::RequesterActivityMountRule,
                             cta::admin::ActivityMountRuleLsItem,
                             decltype(&fillActivityMountRuleItem)>(
        catalogue,
        scheduler,
        instanceName,
        catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules(),
        cta::admin::HeaderType::ACTIVITYMOUNTRULE_LS,
        &fillActivityMountRuleItem) {}

cta::admin::ActivityMountRuleLsItem* ActivityMountRuleLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_amrls_item();
}
} // namespace cta::frontend::grpc