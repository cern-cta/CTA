/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"
#include <memory>

namespace cta {
  class MockRetrieveMount: public cta::RetrieveMount {
  public:
    int getJobs;
    int completes;
    MockRetrieveMount(): getJobs(0), completes(0) {}

    ~MockRetrieveMount() throw() {
    }

    std::unique_ptr<cta::RetrieveJob> getNextJob(log::LogContext & logContext) override {
      getJobs++;
      if(m_jobs.empty()) {
        return std::unique_ptr<cta::RetrieveJob>();
      } else {
        std::unique_ptr<cta::RetrieveJob> job =  std::move(m_jobs.front());
        m_jobs.pop_front();
        return job;
      }
    }

    virtual std::string getMountTransactionId() const override {
      return "1234567890";
    }

    void abort() override { completes ++; }

    void diskComplete() override { completes ++;}

    void tapeComplete() override {};

    bool bothSidesComplete() override { return false; }
    
    void setDriveStatus(cta::common::dataStructures::DriveStatus status) override {};


  private:

    std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;
  public:
    void createRetrieveJobs(const unsigned int nbJobs) {
      for(unsigned int i = 0; i < nbJobs; i++) {
        m_jobs.push_back(std::unique_ptr<cta::RetrieveJob>(
          new MockRetrieveJob(*this)));
      }
    }
  }; // class MockRetrieveMount
}