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

#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"


#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
namespace detail{
  //nameholder
  struct Recall{};
  struct Migration{};
  /**
   * template class without definition == forward declaration
   * that way if we try to use it with another type than the ones
   * it is specialised, you get an compile timne error 
   */
  template <class> struct HelperTrait;
  
  //full template specialisation
 template <> struct HelperTrait<Migration>{
    typedef tapegateway::FileMigrationReportList FileReportList;
    typedef tapegateway::FileToMigrateStruct FileStruct;
    typedef tapegateway::FileMigratedNotificationStruct FileSuccessStruct;
    typedef tapegateway::FileErrorReportStruct FileErrorStruct;
    
  };
  template <> struct HelperTrait<Recall>{
    typedef tapegateway::FileRecallReportList FileReportList;
    typedef tapegateway::FileToRecallStruct FileStruct;
    typedef tapegateway::FileRecalledNotificationStruct FileSuccessStruct;
    typedef tapegateway::FileErrorReportStruct FileErrorStruct;
  };
  
  // Enum describing the type of client. Some clients need batched reports,
  // some prefer reports file by file
  enum ReportBatching {
    ReportInBulk,
    ReportByFile
  };
}

// Forward declaration to avoid circular inclusions.
class TaskWatchDog;

/**
 * Utility class that should be inherited privately/protectedly 
 * the type PlaceHolder is either detail::Recall or detail::Migration
 */
template <class PlaceHolder> class ReportPackerInterface{
  public :
    //some inner typedef to have shorter (and unified) types inside the class
  typedef typename detail::HelperTrait<PlaceHolder>::FileReportList FileReportList;
  typedef typename detail::HelperTrait<PlaceHolder>::FileStruct FileStruct;
  typedef typename detail::HelperTrait<PlaceHolder>::FileSuccessStruct FileSuccessStruct;
  typedef typename detail::HelperTrait<PlaceHolder>::FileErrorStruct FileErrorStruct;
  
  // Pass a reference to the watchdog for initial process reporting.
  void setWatchdog(TaskWatchDog & wd) {
    m_watchdog = &wd;
  }

  protected:
    virtual ~ReportPackerInterface() {}
    ReportPackerInterface(client::ClientInterface & tg, log::LogContext lc):
    m_client(tg),m_lc(lc),m_listReports(new FileReportList),
    m_reportBatching(detail::ReportInBulk),m_watchdog(NULL) {}
  
  /**
   * Log a set of files independently of the success/failure 
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
  template <class C> void logReport(const C& c,const std::string& msg){
    using castor::log::LogContext;
    using castor::log::Param;
      for(typename C::const_iterator it=c.begin();it!=c.end();++it)
      {
        log::ScopedParamContainer sp(m_lc);
        sp.add("NSFILEID",(*it)->fileid())
          .add("NSFSEQ", (*it)->fseq())
          .add("NSHOST", (*it)->nshost())
          .add("NSFILETRANSACTIONID", (*it)->fileTransactionId());
        m_lc.log(LOG_INFO,msg);
      }
  }  

  /**
   * Log a set of files independently of the success/failure 
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
  template <class C> void logReportWithError(const C& c,const std::string& msg){
    using castor::log::LogContext;
    using castor::log::Param;
      for(typename C::const_iterator it=c.begin();it!=c.end();++it)
      {
        log::ScopedParamContainer sp(m_lc);
        sp.add("NSFILEID",(*it)->fileid())
          .add("NSFSEQ", (*it)->fseq())
          .add("NSHOST", (*it)->nshost())
          .add("NSFILETRANSACTIONID", (*it)->fileTransactionId())
          .add("ErrorMessage", (*it)->errorMessage())
          .add("ErrorCode", (*it)->errorCode());
        m_lc.log(LOG_INFO,msg);
      }
  }
  /**
   * Utility function used to log  a ClientInterface::RequestReport 
   * @param chono the time report to log 
   * @param msg the message we want to have in the log 
   * @param level the log level wanted
   */
  void logRequestReport(const client::ClientInterface::RequestReport& chono,
    const std::string& msg, int level=LOG_INFO){
    castor::log::ScopedParamContainer params(m_lc);
    params.add("connectDuration",chono.connectDuration)
          .add("sendRecvDuration",chono.sendRecvDuration)
          .add("transactionId", chono.transactionId);
    m_lc.log(level,msg);
        
  } 
  /**
   * The client of the session to who; we will report
   */
  client::ClientInterface & m_client;
  
  /**
   * The  log context, copied du to threads
   */
  castor::log::LogContext m_lc;
  
  /** 
   * m_listReports is holding all the report waiting to be processed
   */
  std::auto_ptr<FileReportList> m_listReports;
  /**
   * Define how we should report to the client (by file/in bulk).
   */  
  enum detail::ReportBatching m_reportBatching;
  public:

  /**
   * Turn off the packing of the reports by the report packer.
   * This is used for recalls driven by read_tp.
   */
  virtual void disableBulk() { m_reportBatching = detail::ReportByFile; }
  
  /**
   * Pointer to the watchdog, so we can communicate communication errors
   * and end of session results to the initial process
   */
  TaskWatchDog * m_watchdog;
  
};

}}}}


