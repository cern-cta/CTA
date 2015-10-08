/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "scheduler/ArchiveMount.hpp"
#include "scheduler/testingMocks/MockArchiveJob.hpp"

namespace cta {

  class MockArchiveMount: public cta::ArchiveMount {
    public:
      int getJobs;
      int completes;
      MockArchiveMount(cta::NameServer &ns): 
        cta::ArchiveMount(ns), getJobs(0), completes(0) {}

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
      
      void complete() {
        completes++;
      }

    private:

      std::list<std::unique_ptr<cta::ArchiveJob>> m_jobs;
    public:
      void createArchiveJobs(const unsigned int nbJobs) {
        for(unsigned int i = 0; i < nbJobs; i++) {
          m_jobs.push_back(std::unique_ptr<cta::ArchiveJob>(
            new cta::MockArchiveJob(*this, ::cta::ArchiveMount::m_ns)));
        }
      }
    }; // class MockArchiveMount   
}