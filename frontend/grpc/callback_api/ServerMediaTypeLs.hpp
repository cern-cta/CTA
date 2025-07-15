// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "catalogue/MediaTypeWithLogs.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class MediaTypeLsWriteReactor : public TemplateAdminCmdStream<cta::catalogue::MediaTypeWithLogs, cta::admin::MediaTypeLsItem, decltype(&fillMediaTypeItem)> {
    public:
        MediaTypeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        cta::admin::MediaTypeLsItem* getMessageField(cta::xrd::Data* data) override;       
};

MediaTypeLsWriteReactor::MediaTypeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                 cta::Scheduler& scheduler,
                                                 const std::string& instanceName,
                                                 const cta::xrd::Request* request)
    : TemplateAdminCmdStream<cta::catalogue::MediaTypeWithLogs,
                             cta::admin::MediaTypeLsItem,
                             decltype(&fillMediaTypeItem)>(catalogue,
                                                           scheduler,
                                                           instanceName,
                                                           catalogue.MediaType()->getMediaTypes(),
                                                           cta::admin::HeaderType::MEDIATYPE_LS,
                                                           &fillMediaTypeItem) {}

cta::admin::MediaTypeLsItem* MediaTypeLsWriteReactor::getMessageField(cta::xrd::Data* data) {
    return data->mutable_mtls_item();
}

} // namespace cta::frontend::grpc