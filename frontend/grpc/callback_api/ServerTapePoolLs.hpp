// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "catalogue/TapePool.hpp"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class TapePoolLsWriteReactor : public TemplateAdminCmdStream<cta::catalogue::TapePool, cta::admin::TapePoolLsItem, decltype(&fillTapePoolItem)> {
public:
    cta::admin::TapePoolLsItem* getMessageField(cta::xrd::Data* data) override { return data->mutable_tpls_item(); }
    TapePoolLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler,
                            const std::string& instanceName,
                            const cta::xrd::Request* request);

};

TapePoolLsWriteReactor::TapePoolLsWriteReactor(cta::catalogue::Catalogue &catalogue, 
                                              cta::Scheduler &scheduler, 
                                              const std::string& instanceName, 
                                              const cta::xrd::Request* request)
: TemplateAdminCmdStream(
    catalogue, 
    scheduler, 
    instanceName,
    [&catalogue, request]() -> std::list<cta::catalogue::TapePool> {
        using namespace cta::admin;
        request::RequestMessage requestMsg(*request);
        cta::catalogue::TapePoolSearchCriteria searchCriteria;

        searchCriteria.name = requestMsg.getOptional(OptionString::TAPE_POOL);
        searchCriteria.vo = requestMsg.getOptional(OptionString::VO);
        searchCriteria.encrypted = requestMsg.getOptional(OptionBoolean::ENCRYPTED);
        
        return catalogue.TapePool()->getTapePools(searchCriteria);
    }(),
    cta::admin::HeaderType::TAPEPOOL_LS,
    &fillTapePoolItem
) {}
} // namespace cta::frontend::grpc