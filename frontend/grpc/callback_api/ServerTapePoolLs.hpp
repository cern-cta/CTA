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

namespace cta::frontend::grpc {

class TapePoolLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        TapePoolLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In TapePoolLsWriteReactor, we are inside OnWriteDone" << std::endl;
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<cta::catalogue::TapePool> m_tapePoolList;
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::catalogue::TapePool>::const_iterator next_tape_pool;
};

void TapePoolLsWriteReactor::OnDone() {
    std::cout << "In TapePoolLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

TapePoolLsWriteReactor::TapePoolLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request) : m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In TapePoolLsWriteReactor constructor, just entered!" << std::endl;

    request::RequestMessage requestMsg(*request);
    cta::catalogue::TapePoolSearchCriteria searchCriteria;

    searchCriteria.name = requestMsg.getOptional(OptionString::TAPE_POOL);
    searchCriteria.vo = requestMsg.getOptional(OptionString::VO);
    searchCriteria.encrypted = requestMsg.getOptional(OptionBoolean::ENCRYPTED);
  
    m_tapePoolList = catalogue.TapePool()->getTapePools(searchCriteria);

    next_tape_pool = m_tapePoolList.cbegin();
    NextWrite();
}

void TapePoolLsWriteReactor::NextWrite() {
    std::cout << "In TapePoolLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    static int iteration = 0;
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response(); // https://stackoverflow.com/questions/75693340/how-to-set-oneof-field-in-c-grpc-server-and-read-from-client
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::TAPEPOOL_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        std::cout << "header was sent, now entering the loop to send the data, should send " << m_tapePoolList.size() << " records!" << std::endl;
        while(next_tape_pool != m_tapePoolList.cend()) {
            iteration++;
            // cta::xrd::Data record;
            std::cout << "Inside the for loop for the tapes, this is iteration number " << iteration << " and records left are " << m_tapePoolList.size() << std::endl;
            // auto &tape = m_tapePoolList.front();
            const auto& tp = *next_tape_pool;
            ++next_tape_pool;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::TapePoolLsItem *tp_item = data->mutable_tpls_item();
            
            tp_item->set_name(tp.name);
            tp_item->set_vo(tp.vo.name);
            tp_item->set_num_tapes(tp.nbTapes);
            tp_item->set_num_partial_tapes(tp.nbPartialTapes);
            tp_item->set_num_physical_files(tp.nbPhysicalFiles);
            tp_item->set_capacity_bytes(tp.capacityBytes);
            tp_item->set_data_bytes(tp.dataBytes);
            tp_item->set_encrypt(tp.encryption);
            tp_item->set_supply(tp.supply ? tp.supply.value() : "");
            tp_item->mutable_created()->set_username(tp.creationLog.username);
            tp_item->mutable_created()->set_host(tp.creationLog.host);
            tp_item->mutable_created()->set_time(tp.creationLog.time);
            tp_item->mutable_modified()->set_username(tp.lastModificationLog.username);
            tp_item->mutable_modified()->set_host(tp.lastModificationLog.host);
            tp_item->mutable_modified()->set_time(tp.lastModificationLog.time);
            tp_item->set_comment(tp.comment);
            for (auto& source : tp.supply_source_set) {
            tp_item->add_supply_source(source);
            }
            for (auto& destination : tp.supply_destination_set) {
            tp_item->add_supply_destination(destination);
            }

            std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end while
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc