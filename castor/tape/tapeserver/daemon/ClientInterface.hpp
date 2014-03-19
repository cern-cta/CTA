/* 
 * File:   ClientInterface.hpp
 * Author: dcome
 *
 * Created on March 18, 2014, 12:27 PM
 */

#ifndef CLIENTINTERFACE_HPP
#define	CLIENTINTERFACE_HPP

#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "../threading/Threading.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  

class ClientInterface {

public :
      /**
     * Class holding the timing information for the request/reply,
     * and the message sequence Id.
     */
  class RequestReport {
  public:
    RequestReport(): transactionId(0),
            connectDuration(0), sendRecvDuration(0) {}
    uint32_t transactionId;
    double connectDuration;
    double sendRecvDuration;
  };
    
  virtual tapegateway::FilesToRecallList * getFilesToRecall(uint64_t files,
  uint64_t bytes, RequestReport &report)
  throw (castor::tape::Exception) = 0;

  virtual ~ClientInterface(){}
};

}}}}
#endif	/* CLIENTINTERFACE_HPP */

