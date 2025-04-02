// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include "common/dataStructures/JobQueueType.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class FailedRequestLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        FailedRequestLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, SchedulerDatabase &schedDB, cta::log::LogContext lc, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            SendData();
        }
        void OnDone() override;
    private:
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor>  m_archiveQueueItorPtr;     //!< Archive Queue Iterator
        std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> m_retrieveQueueItorPtr;    //!< Retrieve Queue Iterator
        bool m_isSummary; //!< Show only summary of items in the failed queues
        bool m_isSummaryDone; //!< Summary has been sent
        bool m_isLogEntries; //!< Show failure log messages (verbose)
        cta::Scheduler &m_scheduler;    //!< Reference to CTA Scheduler
        cta::log::LogContext m_lc;

        void SendHeader();
        void SendData();

        bool isArchiveJobs() const {
            return m_archiveQueueItorPtr && !m_archiveQueueItorPtr->end();
        }
        
        bool isRetrieveJobs() const {
            return m_retrieveQueueItorPtr && !m_retrieveQueueItorPtr->end();
        }
        void GetBuffSummary();
};

void FailedRequestLsWriteReactor::OnDone() {
    delete this;
}

FailedRequestLsWriteReactor::FailedRequestLsWriteReactor(cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, SchedulerDatabase &schedDB, cta::log::LogContext lc, const cta::xrd::Request* request)
    : m_isHeaderSent(false),
      m_isSummaryDone(false),
      m_scheduler(scheduler),
      m_lc(lc) {
    using namespace cta::admin;

    request::RequestMessage requestMsg(*request);
    m_isSummary = requestMsg.hasFlag(admin::OptionBoolean::SUMMARY);
    m_isLogEntries = requestMsg.hasFlag(admin::OptionBoolean::SHOW_LOG_ENTRIES);

    if (m_isLogEntries && m_isSummary) {
        throw cta::exception::UserError("--log and --summary are mutually exclusive");
    }
    
    auto tapepool     = requestMsg.getOptional(cta::admin::OptionString::TAPE_POOL);
    auto vid          = requestMsg.getOptional(cta::admin::OptionString::VID);
    bool justarchive  = requestMsg.hasFlag(cta::admin::OptionBoolean::JUSTARCHIVE)  || tapepool;
    bool justretrieve = requestMsg.hasFlag(cta::admin::OptionBoolean::JUSTRETRIEVE) || vid;

    if (justarchive && justretrieve) {
        throw cta::exception::UserError("--justarchive/--tapepool and --justretrieve/--vid options are mutually exclusive");
    }

    using common::dataStructures::JobQueueType;
    if (!justretrieve)
        m_archiveQueueItorPtr = schedDB.getArchiveJobQueueItor(tapepool.value_or(""), JobQueueType::FailedJobs);
    if (!justarchive)
        m_retrieveQueueItorPtr = schedDB.getRetrieveJobQueueItor(vid.value_or(""), JobQueueType::FailedJobs);

    std::cout << "In FailedRequestLs constructor, about to send the header" << std::endl;
    SendHeader();
    std::cout << "In FailedRequestLs constructor, about to send the data, should have sent the header" << std::endl;
}

void FailedRequestLsWriteReactor::SendHeader() {
    std::cout << "In the call to send the header" << std::endl;
    m_response.Clear();
    cta::xrd::Response *header = new cta::xrd::Response();
    header->set_type(cta::xrd::Response::RSP_SUCCESS);
    if (m_isSummary) {
        header->set_show_header(cta::admin::HeaderType::FAILEDREQUEST_LS_SUMMARY);
        std::cout << "Will send a failed request summary header" << std::endl;
    }
    else {
        std::cout << "Will send a failed request header " << std::endl;
        header->set_show_header(cta::admin::HeaderType::FAILEDREQUEST_LS);
    }
    m_response.set_allocated_header(header);

    m_isHeaderSent = true;
    StartWrite(&m_response); // this will trigger the OnWriteDone method
}

void FailedRequestLsWriteReactor::SendData() {
    m_response.Clear();
    std::cout << "In SendData(), just entered" << std::endl;
    if (m_isSummary && !m_isSummaryDone) {
        GetBuffSummary();
        Finish(::grpc::Status::OK);
    } else {
        // List failed archive requests
        if (isArchiveJobs()) {
            for (; !m_archiveQueueItorPtr->end(); ++*m_archiveQueueItorPtr) {
                m_response.Clear();
                const common::dataStructures::ArchiveJob &item = **m_archiveQueueItorPtr;
                cta::xrd::Data* data = new cta::xrd::Data();
                cta::admin::FailedRequestLsItem *fr_item = data->mutable_frls_item();
                auto &tapepool = m_archiveQueueItorPtr->qid();

                fr_item->set_object_id(item.objectId);
                fr_item->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
                fr_item->set_tapepool(tapepool);
                fr_item->set_copy_nb(item.copyNumber);
                fr_item->mutable_requester()->set_username(item.request.requester.name);
                fr_item->mutable_requester()->set_groupname(item.request.requester.group);
                fr_item->mutable_af()->set_archive_id(item.archiveFileID);
                fr_item->mutable_af()->set_disk_instance(item.instanceName);
                fr_item->mutable_af()->set_disk_id(item.request.diskFileID);
                fr_item->mutable_af()->set_size(item.request.fileSize);
                fr_item->mutable_af()->set_storage_class(item.request.storageClass);
                fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
                fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
                fr_item->set_totalretries(item.totalRetries);
                fr_item->set_totalreportretries(item.totalReportRetries);
                if (m_isLogEntries) {
                    *fr_item->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
                    *fr_item->mutable_reportfailurelogs() = { item.reportfailurelogs.begin(), item.reportfailurelogs.end() };
                }
                m_response.set_allocated_data(data);
                StartWrite(&m_response);
            }
        }
        if (isRetrieveJobs()) {
            for (; !m_retrieveQueueItorPtr->end(); ++*m_retrieveQueueItorPtr) {
                m_response.Clear();
                const common::dataStructures::RetrieveJob &item = **m_retrieveQueueItorPtr;
                cta::xrd::Data* data = new cta::xrd::Data();
                cta::admin::FailedRequestLsItem *fr_item = data->mutable_frls_item();
                auto &vid = m_archiveQueueItorPtr->qid();

                fr_item->set_object_id(item.objectId);
                fr_item->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
                fr_item->set_copy_nb(item.tapeCopies.at(vid).first);
                fr_item->mutable_requester()->set_username(item.request.requester.name);
                fr_item->mutable_requester()->set_groupname(item.request.requester.group);
                fr_item->mutable_af()->set_archive_id(item.request.archiveFileID);
                fr_item->mutable_af()->set_size(item.fileSize);
                fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
                fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
                fr_item->mutable_tf()->set_vid(vid);
                fr_item->set_totalretries(item.totalRetries);
                fr_item->set_totalreportretries(item.totalReportRetries);

                // Find the correct tape copy
                for (auto &tapecopy : item.tapeCopies) {
                    auto &tf = tapecopy.second.second;
                    if (tf.vid == vid) {
                    fr_item->mutable_tf()->set_f_seq(tf.fSeq);
                    fr_item->mutable_tf()->set_block_id(tf.blockId);
                    break;
                    }
                }

                if (m_isLogEntries) {
                    *fr_item->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
                    *fr_item->mutable_reportfailurelogs() = {item.reportfailurelogs.begin(), item.reportfailurelogs.end()};
                }
                m_response.set_allocated_data(data);
                StartWrite(&m_response);
            } // end for
        } // end if Retrieve, should finish the call here
        // Finish the call
        std::cout << "In FailedRequestLs, SendData(), sent the data, finishing the call on the server side" << std::endl;
        Finish(::grpc::Status::OK);
    }

}

void FailedRequestLsWriteReactor::GetBuffSummary() {
    SchedulerDatabase::JobsFailedSummary archive_summary;
    SchedulerDatabase::JobsFailedSummary retrieve_summary;
    
    if (isArchiveJobs()) {
        m_response.Clear();

        cta::xrd::Data* data = new cta::xrd::Data();
        cta::admin::FailedRequestLsSummary *fr_summary = data->mutable_frls_summary();
        archive_summary = m_scheduler.getArchiveJobsFailedSummary(m_lc);
        fr_summary->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
        fr_summary->set_total_files(archive_summary.totalFiles);
        fr_summary->set_total_size(archive_summary.totalBytes);

        // write the record but don't finish the call, that'll be done by the caller
        m_response.set_allocated_data(data);
        StartWrite(&m_response);
    }
    if (isRetrieveJobs()) {
        m_response.Clear();
        cta::xrd::Data* data = new cta::xrd::Data();
        cta::admin::FailedRequestLsSummary *fr_summary = data->mutable_frls_summary();
        retrieve_summary = m_scheduler.getRetrieveJobsFailedSummary(m_lc);
        fr_summary->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
        fr_summary->set_total_files(retrieve_summary.totalFiles);
        fr_summary->set_total_size(retrieve_summary.totalBytes);
        m_response.set_allocated_data(data);
        StartWrite(&m_response);
    }
    if (isArchiveJobs() && isRetrieveJobs()) {
        m_response.Clear();
        cta::xrd::Data* data = new cta::xrd::Data();
        cta::admin::FailedRequestLsSummary *fr_summary = data->mutable_frls_summary();

        fr_summary->set_request_type(admin::RequestType::TOTAL);
        fr_summary->set_total_files(archive_summary.totalFiles + retrieve_summary.totalFiles);
        fr_summary->set_total_size(archive_summary.totalBytes + retrieve_summary.totalBytes);

        m_response.set_allocated_data(data);
        StartWrite(&m_response);
    }
  
    m_isSummaryDone = true;
}
} // namespace cta::frontend::grpc