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

#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"

#include <memory>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace unitTests{
  using namespace castor::tape;
  const int init_value=-1;
  const unsigned int nbFile=5;

class FakeClient : public castor::tape::tapeserver::client::ClientInterface
{

public:
  FakeClient(int nbCalls=0):m_current(0)
          /*current_succes_migration(init_value),current_failled_migration(init_value),
          current_succes_recall(0),current_failled_recall(0)*/{
    for(int n=0;n<nbCalls;++n) {
      std::unique_ptr<tapegateway::FilesToRecallList> ptr(new tapegateway::FilesToRecallList());
      for(unsigned int i=0;i<nbFile;++i)
      {
        ptr->filesToRecall().push_back(new tapegateway::FileToRecallStruct);
        ptr->filesToRecall().back()->setFileid(i);
        ptr->filesToRecall().back()->setBlockId0(1*i);
        ptr->filesToRecall().back()->setBlockId1(2*i);
        ptr->filesToRecall().back()->setBlockId2(3*i);
        ptr->filesToRecall().back()->setBlockId3(4*i);
        ptr->filesToRecall().back()->setFseq(255+i);
        ptr->filesToRecall().back()->setPath("/tmp/pipo");
        ptr->filesToRecall().back()->setFileTransactionId(666+i);
        ptr->filesToRecall().back()->setFilesToRecallList(ptr.get());
    }
    lists.push_back(ptr.release());
    }
    lists.push_back(NULL);
  }
  virtual void reportMigrationResults(tapegateway::FileMigrationReportList & migrationReport,
  RequestReport& report) {
  }
  
  virtual void reportRecallResults(tapegateway::FileRecallReportList & recallReport,
  RequestReport& report) {
  }
  
   tapegateway::FilesToMigrateList *getFilesToMigrate(uint64_t files, uint64_t bytes, RequestReport &report){
     return NULL;
  }
  //workaround to use assertion inside a function returning something else than void
  //see http://code.google.com/p/googletest/wiki/AdvancedGuide#Assertion_Placement
  void assertion(){ASSERT_EQ(true,static_cast<unsigned int>(m_current)<lists.size());}
  
  virtual tapegateway::FilesToRecallList * getFilesToRecall(uint64_t files,
  uint64_t bytes, RequestReport &report)  
  {
    
    report.transactionId="666";
    report.connectDuration=42;
    report.sendRecvDuration=21;
    assertion();
    return lists[m_current++];
 
  }
  
  virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
  RequestReport &transactionReport)  {
  };
 
  virtual void reportEndOfSession(RequestReport &report)  {}
private:
  std::vector<tapegateway::FilesToRecallList*> lists;
  int m_current;
};




    
  class MockClient : public castor::tape::tapeserver::client::ClientInterface {
  public:
    MOCK_METHOD3(getFilesToRecall, tapegateway::FilesToRecallList* (uint64_t files,uint64_t bytes, RequestReport &report));
    MOCK_METHOD2(reportMigrationResults, void (tapegateway::FileMigrationReportList & migrationReport,RequestReport &report));
    
    MOCK_METHOD3(reportEndOfSessionWithError, void(const std::string & errorMsg, int errorCode, RequestReport &transactionReport));
    MOCK_METHOD1(reportEndOfSession, void(RequestReport &transactionReport));
    MOCK_METHOD2(reportRecallResults,void(tapegateway::FileRecallReportList & recallReport,RequestReport &report));
    MOCK_METHOD3(getFilesToMigrate,tapegateway::FilesToMigrateList *(uint64_t, uint64_t, RequestReport &));
  };
  
}


