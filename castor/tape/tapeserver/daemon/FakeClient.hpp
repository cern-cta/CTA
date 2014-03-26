/* 
 * File:   FakeClient.hpp
 * Author: dcome
 *
 * Created on March 26, 2014, 9:38 AM
 */

#ifndef FAKECLIENT_HPP
#define	FAKECLIENT_HPP

#include "castor/tape/tapeserver/daemon/ClientInterface.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include <memory>
#include <gtest/gtest.h>

namespace{
  using namespace castor::tape;
  const int init_value=-1;
  const unsigned int nbFile=5;

class FakeClient : public castor::tape::tapeserver::daemon::ClientInterface
{

public:
  FakeClient(int nbCalls=0):m_current(0),
          current_succes_migration(init_value),current_failled_migration(init_value),
          current_succes_recall(0),current_failled_recall(0){
    for(int n=0;n<nbCalls;++n) {
      std::auto_ptr<tapegateway::FilesToRecallList> ptr(new tapegateway::FilesToRecallList());
      for(unsigned int i=0;i<nbFile;++i)
      {
        ptr->filesToRecall().push_back(new tapegateway::FileToRecallStruct);
        ptr->filesToRecall().back()->setFileid(i);
        ptr->filesToRecall().back()->setBlockId0(1*i);
        ptr->filesToRecall().back()->setBlockId1(2*i);
        ptr->filesToRecall().back()->setBlockId2(3*i);
        ptr->filesToRecall().back()->setBlockId3(4*i);
        ptr->filesToRecall().back()->setFseq(255+i);
        ptr->filesToRecall().back()->setPath("rfio:///root/bidule");
        ptr->filesToRecall().back()->setFileTransactionId(666+i);
        ptr->filesToRecall().back()->setFilesToRecallList(ptr.get());
    }
    lists.push_back(ptr.release());
    }
    lists.push_back(NULL);
  }
  virtual void reportMigrationResults(tapegateway::FileMigrationReportList & migrationReport,
  RequestReport& report) throw (castor::tape::Exception){
    current_succes_migration=migrationReport.successfulMigrations().size();
    current_failled_migration=migrationReport.failedMigrations().size();
  }
  
  virtual void reportRecallResults(tapegateway::FileRecallReportList & recallReport,
  RequestReport& report) throw (castor::tape::Exception){
    current_succes_recall=recallReport.successfulRecalls().size();
    current_failled_recall=recallReport.failedRecalls().size();
  }
  
  //workaround to use assertion inside a function returning something else than void
  //see http://code.google.com/p/googletest/wiki/AdvancedGuide#Assertion_Placement
  void assertion(){ASSERT_EQ(true,static_cast<unsigned int>(m_current)<lists.size());}
  
  virtual tapegateway::FilesToRecallList * getFilesToRecall(uint64_t files,
  uint64_t bytes, RequestReport &report) throw (castor::tape::Exception) 
  {
    
    report.transactionId=666;
    report.connectDuration=42;
    report.sendRecvDuration=21;
    assertion();
    return lists[m_current++];
 
  }
  
  virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
  RequestReport &transactionReport) throw (castor::tape::Exception) {
    error_code=errorCode;
    msg_error=errorMsg;
  };
 
  virtual void reportEndOfSession(RequestReport &report) throw (Exception) {}
private:
  std::vector<tapegateway::FilesToRecallList*> lists;
  int m_current;
public:
  int current_succes_migration;
  int current_failled_migration;
  
  int current_succes_recall;
  int current_failled_recall;
  
  int error_code;
  std::string msg_error;
};
}

#endif	/* FAKECLIENT_HPP */

