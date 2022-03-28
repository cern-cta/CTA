/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "scheduler/ArchiveMount.hpp"
#include "scheduler/testingMocks/MockArchiveJob.hpp"

namespace cta {

  class MockArchiveMount: public cta::ArchiveMount {
    public:
      int getJobs;
      int completes;
      MockArchiveMount(cta::catalogue::Catalogue &catalogue): 
        cta::ArchiveMount(catalogue), getJobs(0), completes(0) {}

      ~MockArchiveMount() throw() {
      }

      std::unique_ptr<cta::ArchiveJob> getNextJob() {
        getJobs++;
        if(m_jobs.empty()) {
          return std::unique_ptr<cta::ArchiveJob>();
        } else {
          std::unique_ptr<cta::ArchiveJob> job =  std::move(m_jobs.front());
          m_jobs.pop_front();
          return job;
        }
      }
      
      void reportJobsBatchTransferred(std::queue<std::unique_ptr<cta::ArchiveJob> >& successfulArchiveJobs, 
          std::queue<cta::catalogue::TapeItemWritten> & skippedFiles, std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& failedToReportArchiveJobs, cta::log::LogContext& logContext) override {
        bool catalogue_updated = false;
        try {
          std::set<cta::catalogue::TapeItemWrittenPointer> tapeItemsWritten;
          std::list<std::unique_ptr<cta::ArchiveJob> > validatedSuccessfulArchiveJobs;
          std::unique_ptr<cta::ArchiveJob> job;
          while(!successfulArchiveJobs.empty()) {
            // Get the next job to report and make sure we will not attempt to process it twice.
            job = std::move(successfulArchiveJobs.front());
            successfulArchiveJobs.pop();
            if (!job.get()) continue;        
            tapeItemsWritten.emplace(job->validateAndGetTapeFileWritten());
            validatedSuccessfulArchiveJobs.emplace_back(std::move(job));      
            job.reset(nullptr);
          }
          while (!skippedFiles.empty()) {
            auto tiwup = cta::make_unique<cta::catalogue::TapeItemWritten>();
            *tiwup = skippedFiles.front();
            skippedFiles.pop();
            tapeItemsWritten.emplace(tiwup.release());
          }
          m_catalogue.filesWrittenToTape(tapeItemsWritten);
          catalogue_updated = true;
          for (auto &job: validatedSuccessfulArchiveJobs) {
            MockArchiveJob * maj = dynamic_cast<MockArchiveJob *>(job.get());
            if (!maj) throw cta::exception::Exception("Wrong job type.");
            maj->reportJobSucceeded();
            logContext.log(log::INFO, "Reported to the client a full file archival.");
          }
          logContext.log(log::INFO, "Reported to the client that a batch of files was written on tape");
        } catch(const cta::exception::Exception& e){
          cta::log::ScopedParamContainer params(logContext);
          params.add("exceptionMessageValue", e.getMessageValue());
          const std::string msg_error="In ArchiveMount::reportJobsBatchWritten(): got an exception";
          logContext.log(cta::log::ERR, msg_error);
          if (catalogue_updated) {
            throw cta::ArchiveMount::FailedReportMoveToQueue(msg_error);
          }
          else {
            throw cta::ArchiveMount::FailedReportCatalogueUpdate(msg_error);
          }
        }
      }

      void complete() override {
        completes++;
      }

    private:

      std::list<std::unique_ptr<cta::ArchiveJob>> m_jobs;
    public:
      void createArchiveJobs(const unsigned int nbJobs) {
        for(unsigned int i = 0; i < nbJobs; i++) {
          m_jobs.push_back(std::unique_ptr<cta::ArchiveJob>(
            new cta::MockArchiveJob(this, ::cta::ArchiveMount::m_catalogue)));
        }
      }
    }; // class MockArchiveMount   
}