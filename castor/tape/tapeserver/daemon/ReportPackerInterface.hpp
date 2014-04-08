/******************************************************************************
 *                      ReportPackerInterface.hpp
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

#ifndef REPORTPACKERINTERFACE_HPP
#define	REPORTPACKERINTERFACE_HPP

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
  struct Recall{};
  struct Migration{};
  
  template <class> struct HelperTrait;
  
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
}
  
template <class PlaceHolder> class ReportPackerInterface{
public:
  typedef typename detail::HelperTrait<PlaceHolder>::FileReportList FileReportList;
  typedef typename detail::HelperTrait<PlaceHolder>::FileStruct FileStruct;
  typedef typename detail::HelperTrait<PlaceHolder>::FileSuccessStruct FileSuccessStruct;
  typedef typename detail::HelperTrait<PlaceHolder>::FileErrorStruct FileErrorStruct;
  

  ReportPackerInterface(client::ClientInterface & tg,log::LogContext lc):
  m_client(tg),m_lc(lc),m_listReports(new FileReportList)
  {}
  
  virtual ~ReportPackerInterface(){}
  
  /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   */
  virtual void reportCompletedJob(const FileStruct&) =0;
  
  /**
   * Create into the MigrationReportPacker a report for the failled migration
   * of migratedFile
   * @param migratedFile the file which failled 
   */
  virtual void reportFailedJob(const FileStruct& ,const std::string&,int)=0;
       
  /**
   * Create into the MigrationReportPacker a report for the nominal end of session
   */
  virtual void reportEndOfSession() =0;
  
  /**
   * Create into the MigrationReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   */
  virtual void reportEndOfSessionWithErrors(const std::string,int) =0;
  
  
  protected:
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
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("NSFILEID",(*it)->fileid())),
          LogContext::ScopedParam(m_lc, Param("NSFSEQ", (*it)->fseq())),
          LogContext::ScopedParam(m_lc, Param("NSHOST", (*it)->nshost())),
          LogContext::ScopedParam(m_lc, Param("NSFILETRANSACTIONID", (*it)->fileTransactionId()))
        };
        tape::utils::suppresUnusedVariable(sp);
        m_lc.log(LOG_INFO,msg);
      }
  }  
  
  void logRequestReport(const client::ClientInterface::RequestReport& chono,const std::string& msg,int level=LOG_INFO){
    using castor::log::LogContext;
    using castor::log::Param;
     
        LogContext::ScopedParam sp[]={
          LogContext::ScopedParam(m_lc, Param("connectDuration",chono.connectDuration)),
          LogContext::ScopedParam(m_lc, Param("sendRecvDuration",chono.sendRecvDuration)),
          LogContext::ScopedParam(m_lc, Param("transactionId", chono.transactionId)),
        };
        tape::utils::suppresUnusedVariable(sp);
        m_lc.log(level,msg);
        
  } 
  client::ClientInterface & m_client;
  castor::log::LogContext m_lc;
  
  /** 
   * m_listReports is holding all the report waiting to be processed
   */
  std::auto_ptr<FileReportList> m_listReports;   
};

}}}}

#endif	/* REPORTPACKERINTERFACE_HPP */

