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
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include <gtest/gtest.h>

namespace unitTests{
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::tapeserver::client;
  struct MockRecallReportPacker : public RecallReportPacker {
    MOCK_METHOD3(reportCompletedJob,void(const FileStruct&,u_int32_t,u_int64_t));
    MOCK_METHOD3(reportFailedJob, void(const FileStruct& ,const std::string&,int));
    MOCK_METHOD0(reportEndOfSession, void());
    MOCK_METHOD2(reportEndOfSessionWithErrors, void(const std::string,int));
    MockRecallReportPacker(ClientInterface& client,castor::log::LogContext lc):
    RecallReportPacker(client,1,lc){}
  };
  
  struct MockTaskInjector : public RecallTaskInjector{
    MOCK_METHOD3(requestInjection, void(int maxFiles, int maxBlocks, bool lastCall));
  };
  
  TEST(castor_tape_tapeserver_daemon, DiskWriteThreadPoolTest){
    using ::testing::_;
    MockClient client;
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_DiskWriteThreadPoolTest");
    castor::log::LogContext lc(log);
    
    MockRecallReportPacker report(client,lc);
    EXPECT_CALL(report,reportCompletedJob(_,_,_)).Times(5);
    //EXPECT_CALL(tskInjectorl,requestInjection(_,_,_)).Times(2);
    EXPECT_CALL(report,reportEndOfSession()).Times(1);     
    
    RecallMemoryManager mm(10,100,lc);
    
    DiskWriteThreadPool dwtp(2,report,*((RecallWatchDog*)NULL),lc,"RFIO","/dev/null",0);
    dwtp.startThreads();
    
    castor::tape::tapegateway::FileToRecallStruct file;
    file.setPath("/dev/null");
    file.setBlockId3(1);
       
     for(int i=0;i<5;++i){
       DiskWriteTask* t=new DiskWriteTask(dynamic_cast<tapegateway::FileToRecallStruct*>(file.clone()),mm);
       MemBlock* mb=mm.getFreeBlock();
       mb->m_fileid=0;
       mb->m_fileBlock=0;
       t->pushDataBlock(mb);
       t->pushDataBlock(NULL);
       dwtp.push(t);
     }
     
    dwtp.finish();
    dwtp.waitThreads(); 
  }
}

