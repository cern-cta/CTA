#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class FailedRequestLsResponseStream : public CtaAdminResponseStream {
public:
    FailedRequestLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                  cta::Scheduler& scheduler, 
                                  const std::string& instanceName,
                                  SchedulerDatabase& schedDb,
                                  cta::log::LogContext& lc);
        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    SchedulerDatabase& m_schedDb;
    cta::log::LogContext& m_lc;
    
    // Configuration options
    bool m_isSummary;
    bool m_isLogEntries;
    std::optional<std::string> m_schedulerBackendName;
    
    // Data storage
    std::list<cta::xrd::Data> m_items;
    
    // Helper methods
    void collectArchiveJobs(const std::optional<std::string>& tapepool);
    void collectRetrieveJobs(const std::optional<std::string>& vid);
    void collectSummaryData(bool hasArchive, bool hasRetrieve);
};

FailedRequestLsResponseStream::FailedRequestLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                             cta::Scheduler& scheduler, 
                                                             const std::string& instanceName,
                                                             SchedulerDatabase& schedDb,
                                                             cta::log::LogContext& lc)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName),
      m_schedDb(schedDb),
      m_lc(lc),
      m_isSummary(false),
      m_isLogEntries(false) {
}

void FailedRequestLsResponseStream::init(const admin::AdminCmd& admincmd) {
    using namespace cta::admin;
    
    cta::frontend::AdminCmdOptions request;
    request.importOptions(admincmd);
    
    // Parse options
    m_isSummary = request.has_flag(OptionBoolean::SUMMARY);
    m_isLogEntries = request.has_flag(OptionBoolean::SHOW_LOG_ENTRIES);
    
    if (m_isLogEntries && m_isSummary) {
        throw std::invalid_argument("--log and --summary are mutually exclusive");
    }
    
    m_schedulerBackendName = m_scheduler.getSchedulerBackendName();
    
    auto tapepool = request.getOptional(OptionString::TAPE_POOL);
    auto vid = request.getOptional(OptionString::VID);
    bool justarchive = request.has_flag(OptionBoolean::JUSTARCHIVE) || tapepool.has_value();
    bool justretrieve = request.has_flag(OptionBoolean::JUSTRETRIEVE) || vid.has_value();
    
    if (justarchive && justretrieve) {
        throw std::invalid_argument("--justarchive/--tapepool and --justretrieve/--vid options are mutually exclusive");
    }
    
    if (m_isSummary) {
        collectSummaryData(!justretrieve, !justarchive);
    } else {
        if (!justretrieve) {
            collectArchiveJobs(tapepool);
        }
        if (!justarchive) {
            collectRetrieveJobs(vid);
        }
    }
}

void FailedRequestLsResponseStream::collectArchiveJobs(const std::optional<std::string>& tapepool) {
    using common::dataStructures::JobQueueType;
    
    auto archiveQueueItor = m_schedDb.getArchiveJobQueueItor(
        tapepool ? *tapepool : "", JobQueueType::FailedJobs);
    
    for (; !archiveQueueItor->end(); ++*archiveQueueItor) {
        auto& tapePoolName = archiveQueueItor->qid();
        const auto& item = **archiveQueueItor;
        
        cta::xrd::Data data;
        auto fr_item = data.mutable_frls_item();

        fr_item->set_object_id(item.objectId);
        fr_item->set_request_type(cta::admin::RequestType::ARCHIVE_REQUEST);
        fr_item->set_tapepool(tapePoolName);
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
        fr_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
        fr_item->set_instance_name(m_instanceName);
        
        m_items.emplace_back(std::move(data));
    }
}

void FailedRequestLsResponseStream::collectRetrieveJobs(const std::optional<std::string>& vid) {
    using common::dataStructures::JobQueueType;
    
    auto retrieveQueueItor = m_schedDb.getRetrieveJobQueueItor(
        vid ? *vid : "", JobQueueType::FailedJobs);
    
    for (; !retrieveQueueItor->end(); ++*retrieveQueueItor) {
        auto& vid = retrieveQueueItor->qid();
        const auto& item = **retrieveQueueItor;
        
        cta::xrd::Data data;
        auto fr_item = data.mutable_frls_item();

        fr_item->set_object_id(item.objectId);
        fr_item->set_request_type(cta::admin::RequestType::RETRIEVE_REQUEST);
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
        fr_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
        fr_item->set_instance_name(m_instanceName);
        
        m_items.emplace_back(std::move(data));
    }
}

void FailedRequestLsResponseStream::collectSummaryData(bool hasArchive, bool hasRetrieve) {
    SchedulerDatabase::JobsFailedSummary archive_summary;
    SchedulerDatabase::JobsFailedSummary retrieve_summary;
    
    if (hasArchive) {
        archive_summary = m_scheduler.getArchiveJobsFailedSummary(m_lc);
        
        cta::xrd::Data data;
        data.mutable_frls_summary()->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
        data.mutable_frls_summary()->set_total_files(archive_summary.totalFiles);
        data.mutable_frls_summary()->set_total_size(archive_summary.totalBytes);
        data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
        data.mutable_frls_summary()->set_instance_name(m_instanceName);
        
        m_items.emplace_back(std::move(data));
    }
    
    if (hasRetrieve) {
        retrieve_summary = m_scheduler.getRetrieveJobsFailedSummary(m_lc);
        
        cta::xrd::Data data;
        data.mutable_frls_summary()->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
        data.mutable_frls_summary()->set_total_files(retrieve_summary.totalFiles);
        data.mutable_frls_summary()->set_total_size(retrieve_summary.totalBytes);
        data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
        data.mutable_frls_summary()->set_instance_name(m_instanceName);
        
        m_items.emplace_back(std::move(data));
    }
    
    if (hasArchive && hasRetrieve) {
        cta::xrd::Data data;
        data.mutable_frls_summary()->set_request_type(admin::RequestType::TOTAL);
        data.mutable_frls_summary()->set_total_files(archive_summary.totalFiles + retrieve_summary.totalFiles);
        data.mutable_frls_summary()->set_total_size(archive_summary.totalBytes + retrieve_summary.totalBytes);
        data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
        data.mutable_frls_summary()->set_instance_name(m_instanceName);
        
        m_items.emplace_back(std::move(data));
    }
}

bool FailedRequestLsResponseStream::isDone() {
    return m_items.empty();
}

cta::xrd::Data FailedRequestLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    cta::xrd::Data data = std::move(m_items.front());
    m_items.pop_front();
    
    return data;
}

} // namespace cta::cmdline