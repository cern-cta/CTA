// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"
#include "TemplateAdminCmdStream.hpp"

namespace cta::frontend::grpc {

class ShowQueuesWriteReactor : public TemplateAdminCmdStream<cta::common::dataStructures::QueueAndMountSummary,
                                                             cta::admin::ShowQueuesItem,
                                                             decltype(&fillSqItem),
                                                             std::string> {
public:
  cta::admin::ShowQueuesItem* getMessageField(cta::xrd::Data* data) override;
  ShowQueuesWriteReactor(cta::catalogue::Catalogue& catalogue,
                         cta::Scheduler& scheduler,
                         const std::string& instanceName,
                         cta::log::LogContext lc,
                         const cta::xrd::Request* request);
};

ShowQueuesWriteReactor::ShowQueuesWriteReactor(cta::catalogue::Catalogue& catalogue,
                                               cta::Scheduler& scheduler,
                                               const std::string& instanceName,
                                               cta::log::LogContext lc,
                                               const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::common::dataStructures::QueueAndMountSummary,
                             cta::admin::ShowQueuesItem,
                             decltype(&fillSqItem),
                             std::string>(catalogue,
                                          scheduler,
                                          instanceName,
                                          scheduler.getQueuesAndMountSummaries(lc),
                                          cta::admin::HeaderType::SHOWQUEUES,
                                          &fillSqItem,
                                          scheduler.getSchedulerBackendName()) {}

cta::admin::ShowQueuesItem* ShowQueuesWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_sq_item();
}

} // namespace cta::frontend::grpc