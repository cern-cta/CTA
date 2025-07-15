// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class MountPolicyLsWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::MountPolicy,
                                                                cta::admin::MountPolicyLsItem,
                                                                decltype(&fillMountPolicyItem)> {
public:
  cta::admin::MountPolicyLsItem* getMessageField(cta::xrd::Data* data) override;
  MountPolicyLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler,
                            const std::string& instanceName,
                            const cta::xrd::Request* request);
};

MountPolicyLsWriteReactor::MountPolicyLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler,
                                                     const std::string& instanceName,
                                                     const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::MountPolicy,
                             cta::admin::MountPolicyLsItem,
                             decltype(&fillMountPolicyItem)>(catalogue,
                                                             scheduler,
                                                             instanceName,
                                                             catalogue.MountPolicy()->getMountPolicies(),
                                                             cta::admin::HeaderType::MOUNTPOLICY_LS,
                                                             &fillMountPolicyItem) {}

cta::admin::MountPolicyLsItem* MountPolicyLsWriteReactor::getMessageField(cta::xrd::Data* data) {
  return data->mutable_mpls_item();
}
}  // namespace cta::frontend::grpc