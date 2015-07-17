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

#include "TapeMount.hpp"
#include "SchedulerDatabase.hpp"
#include "common/exception/Exception.hpp"
#include <memory>

namespace cta {
  
  class ArchiveMount;
  /**
   * The class driving a retrieve mount.
   * The class only has private constructors as it is instanciated by
   * the Scheduler class.
   */
  class ArchiveMount: public TapeMount {
    friend class Scheduler;
  private:
    ArchiveMount(std::unique_ptr<cta::SchedulerDatabase::TapeMount> &mount);
  public:
    CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
    CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
    /**
     * Notifies the scheduler that the session is finished 
     */
    virtual void finish();
    
    /**
     * Job factory
     */
    std::unique_ptr<ArchiveMount> getNextJob();
    
    virtual ~ArchiveMount();
  private:
    std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> m_dbMount;
    
    
  };
}
