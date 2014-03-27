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
namespace client {
  

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
  
  /**
   * Class holding the result of a Volume request
   */
    class VolumeInfo {
    public:
      VolumeInfo() {};
      /** The VID we will work on */
      std::string vid;
      /** The type of the session */
      tapegateway::ClientType clientType;
      /** The density of the volume */
      std::string density;
      /** The label field seems to be in disuse */
      std::string labelObsolete;
      /** The read/write mode */
      tapegateway::VolumeMode volumeMode;
    };
    
    virtual tapegateway::FilesToRecallList* getFilesToRecall(uint64_t files,
    uint64_t bytes, RequestReport &report)  = 0;

    /**
     * Reports the result of migrations to the client.
     * Detailed interface is still TBD.
     * @param report Placeholder to network timing information
     */
    virtual void reportMigrationResults(tapegateway::FileMigrationReportList & migrationReport,
    RequestReport &report)  =0;
    
      /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     * @param transactionReport Placeholder to network timing information,
     * populated during the call and used by the caller to log performance 
     * and context information
     * @param errorMsg (sent to the client)
     * @param errorCode (sent to the client)
     */
    virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
    RequestReport &transactionReport)  = 0;
    
    
    /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     */
    virtual void reportEndOfSession(RequestReport &report) = 0;
    
        /**
     * Reports the result of recall to the client.
     * Detailed interface is still TBD.
     * @param report Placeholder to network timing information
     */
    virtual void reportRecallResults(tapegateway::FileRecallReportList & recallReport,
      RequestReport &report)  =0;
    
    virtual ~ClientInterface(){}
};

}}}}
#endif	/* CLIENTINTERFACE_HPP */

