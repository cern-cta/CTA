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
 * A thread used by the MetricsCollector container/thread pool 
 * to update all collected metric values.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/server/IThread.hpp"
#include "castor/exception/Exception.hpp"

namespace castor  {
namespace server  {
namespace metrics {

const int DEFAULT_SAMPLING_INTERVAL = 30;
    
/**
 * The metrics updater thread
 */
class UpdateThread : public IThread {

public:    
  /**
   * Default constructor. Note: this class uses
   * member thread-UNsafe variables and can't (+ doesn't
   * need to) be run by multiple parallel threads.
   */
  UpdateThread();
  
  /// Default destructor
  virtual ~UpdateThread() throw() {};
  
  /// Standard init method. Initializes m_t0
  virtual void init();
  
  /// Main work for this thread. Updates all metrics
  virtual void run(void* param);
  
  /// Empty stop method
  virtual void stop() {};
  
private:
  
  /// Sampling interval
  int m_sampling;
  
  /// time at previous update
  u_signed64 m_t0;
  
}; // class UpdateThread
    
} // namespace metrics
} // namespace server
} // namespace castor

