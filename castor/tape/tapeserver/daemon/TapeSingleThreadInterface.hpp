/* 
 * File:   TapeSingleThreadInterface.hpp
 * Author: dcome
 *
 * Created on March 18, 2014, 4:28 PM
 */

#pragma once

#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/daemon/TaskInjector.hpp"
#include "castor/log/LogContext.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
template <class Task>
class TapeSingleThreadInterface : private castor::tape::threading::Thread
{
protected:
  castor::tape::threading::BlockingQueue<Task *> m_tasks;
  castor::tape::drives::DriveInterface & m_drive;
  size_t m_filesProcessed;
  std::string m_vid;
  castor::log::LogContext m_logContext;
  castor::tape::tapeserver::daemon::TaskInjector * m_taskInjector;
public:
  void finish() { m_tasks.push(NULL); }
  void push(Task * t) { m_tasks.push(t); }
  
  virtual void startThreads(){ start(); }
  virtual void waitThreads() { wait(); } 
  
  TapeSingleThreadInterface(castor::tape::drives::DriveInterface & drive,
    const std::string & vid, castor::log::LogContext & lc):
  m_drive(drive), m_filesProcessed(0), m_vid(vid), m_logContext(lc) {}
};

}
}
}
}

