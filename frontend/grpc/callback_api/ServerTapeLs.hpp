// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "catalogue/TapeSearchCriteria.hpp"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class TapeLsWriteReactor : public TemplateAdminCmdStream<common::dataStructures::Tape, cta::admin::TapeLsItem, decltype(&fillTapeItem)> {
public:
    cta::admin::TapeLsItem* getMessageField(cta::xrd::Data* data) override { return data->mutable_tals_item(); }
    TapeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                     cta::Scheduler& scheduler,
                     const std::string& instanceName,
                     const cta::xrd::Request* request)
      : TemplateAdminCmdStream<common::dataStructures::Tape, cta::admin::TapeLsItem, decltype(&fillTapeItem)>(
          catalogue,
          scheduler,
          instanceName,
          buildTapeList(catalogue, request),
          cta::admin::HeaderType::TAPE_LS,
          &fillTapeItem) {}

private:
    static std::list<common::dataStructures::Tape> buildTapeList(cta::catalogue::Catalogue& catalogue,
                                                                const cta::xrd::Request* request) {
    using namespace cta::admin;

    cta::catalogue::TapeSearchCriteria searchCriteria;
    request::RequestMessage requestMsg(*request);

    bool has_any = false;  // set to true if at least one optional option is set

    // Get the search criteria from the optional options
    searchCriteria.full = requestMsg.getOptional(OptionBoolean::FULL, &has_any);
    searchCriteria.fromCastor = requestMsg.getOptional(OptionBoolean::FROM_CASTOR, &has_any);
    searchCriteria.capacityInBytes = requestMsg.getOptional(OptionUInt64::CAPACITY, &has_any);
    searchCriteria.logicalLibrary = requestMsg.getOptional(OptionString::LOGICAL_LIBRARY, &has_any);
    searchCriteria.tapePool = requestMsg.getOptional(OptionString::TAPE_POOL, &has_any);
    searchCriteria.vo = requestMsg.getOptional(OptionString::VO, &has_any);
    searchCriteria.vid = requestMsg.getOptional(OptionString::VID, &has_any);
    searchCriteria.mediaType = requestMsg.getOptional(OptionString::MEDIA_TYPE, &has_any);
    searchCriteria.vendor = requestMsg.getOptional(OptionString::VENDOR, &has_any);
    searchCriteria.purchaseOrder = requestMsg.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
    searchCriteria.physicalLibraryName = requestMsg.getOptional(OptionString::PHYSICAL_LIBRARY, &has_any);
    searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
    auto stateOpt = requestMsg.getOptional(OptionString::STATE, &has_any);
    if (stateOpt) {
        searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
    }

    // Validation logic
    if (!(requestMsg.has_flag(OptionBoolean::ALL) || has_any)) {
        std::cout << "The --all flag was not specified, will finish the call" << std::endl;
        throw std::invalid_argument("Must specify at least one search option, or --all"); // need to somehow indicate failure here, call Finish with an error code
    } else if (requestMsg.has_flag(OptionBoolean::ALL) && has_any) {
        std::cout << "The --all flag was specified together with other search options, will finish the call" << std::endl;
        throw std::invalid_argument("Cannot specify --all together with other search options");
    } // exceptions caught in the calling code in CtaAdminServer.hpp

    std::cout << "Calling getTapes to populate the tape list" << std::endl;
    return catalogue.Tape()->getTapes(searchCriteria);
}
};

}  // namespace cta::frontend::grpc