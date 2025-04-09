// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "catalogue/TapeSearchCriteria.hpp"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class TapeLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        TapeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<common::dataStructures::Tape> m_tapeList;
        std::list<common::dataStructures::Tape>::const_iterator next_tape;// &tape = m_tapeList.front();
};

// maybe also override the OnCancel method - Jacek implemented this

// constructor does not make the first call to write, currently.
// In the example's Lister case, the first call to write is made in the constructor
TapeLsWriteReactor::TapeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
  : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    // all this will go into a common method in a base class called
    // getTapesList or something
    using namespace cta::admin;

    cta::catalogue::TapeSearchCriteria searchCriteria;
    request::RequestMessage requestMsg(*request);

    bool has_any = false; // set to true if at least one optional option is set

    // Get the search criteria from the optional options

    searchCriteria.full                = requestMsg.getOptional(OptionBoolean::FULL,                       &has_any);
    searchCriteria.fromCastor          = requestMsg.getOptional(OptionBoolean::FROM_CASTOR,                &has_any);
    searchCriteria.capacityInBytes     = requestMsg.getOptional(OptionUInt64::CAPACITY,                    &has_any);
    searchCriteria.logicalLibrary      = requestMsg.getOptional(OptionString::LOGICAL_LIBRARY,             &has_any);
    searchCriteria.tapePool            = requestMsg.getOptional(OptionString::TAPE_POOL,                   &has_any);
    searchCriteria.vo                  = requestMsg.getOptional(OptionString::VO,                          &has_any);
    searchCriteria.vid                 = requestMsg.getOptional(OptionString::VID,                         &has_any);
    searchCriteria.mediaType           = requestMsg.getOptional(OptionString::MEDIA_TYPE,                  &has_any);
    searchCriteria.vendor              = requestMsg.getOptional(OptionString::VENDOR,                      &has_any);
    searchCriteria.purchaseOrder       = requestMsg.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
    searchCriteria.physicalLibraryName = requestMsg.getOptional(OptionString::PHYSICAL_LIBRARY,            &has_any);
    searchCriteria.diskFileIds         = requestMsg.getOptional(OptionStrList::FILE_ID,                    &has_any);
    auto stateOpt                      = requestMsg.getOptional(OptionString::STATE,                       &has_any);
    if(stateOpt){
        searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
    }
    if(!(requestMsg.has_flag(OptionBoolean::ALL) || has_any)) {
        std::cout << "The --all flag was not specified, will finish the call" << std::endl;
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, "Must specify at least one search option, or --all"));
        return;
    } else if(requestMsg.has_flag(OptionBoolean::ALL) && has_any) {
        std::cout << "The --all flag was specified together with other search options, will finish the call" << std::endl;
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, "Cannot specify --all together with other search options"));
        return;
    }

    std::cout << "Calling getTapes to populate the m_tapeList" << std::endl;
    m_tapeList = catalogue.Tape()->getTapes(searchCriteria);
    next_tape = m_tapeList.cbegin();
    NextWrite();
}

void TapeLsWriteReactor::NextWrite() {
    std::cout << "In TapeLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response(); // https://stackoverflow.com/questions/75693340/how-to-set-oneof-field-in-c-grpc-server-and-read-from-client
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::TAPE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        std::cout << "header was sent, now entering the loop to send the data, should send " << m_tapeList.size() << " records!" << std::endl;
        // for(; !m_tapeList.empty(); m_tapeList.pop_front()) {
        while(next_tape != m_tapeList.cend()) {
            // cta::xrd::Data record;
            // auto &tape = m_tapeList.front();
            const auto& tape = *next_tape;
            ++next_tape;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::TapeLsItem *tape_item = data->mutable_tals_item();
            // cta::admin::TapeLsItem* tape_item = m_response.mutable_data()->mutable_tals_item();
            // cta::xrd::Data* data = new cta::xrd::Data()
            // data.set_allocated_tals_item
            fillTapeItem(tape, tape_item, m_instanceName);
            std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
            // data->set_allocated_tals_item(tape_item);
            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end for
        // did not write anything
        // apparently we never get here, m_tapeList does not change. WTF
        // end while, now finish the call
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
        // if (m_tapeList.empty()) {
        //     std::cout << "Finishing the call on the server side" << std::endl;
        //     // Finish the call
        //     Finish(::grpc::Status::OK);
        // }
    }
}
} // namespace cta::frontend::grpc