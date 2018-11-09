/*!
 * @project        XRootD SSI/Protocol Buffer Interface Project
 * @brief          XRootD SSI Responder class implementation
 * @copyright      Copyright 2018 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "XrdSsiPbConfig.hpp"
#include <sstream>
#include <XrdSsi/XrdSsiEntity.hh>
#include "XrdSsiPbLog.hpp"
#include "XrdSsiPbException.hpp"
#include "XrdSsiPbRequestProc.hpp"
#include "RmcdServiceProvider.hpp"
#include <iostream>
#include "rmc_smcsubr.h"
#include "rmc_smcsubr2.h"
#include "smc_struct.h"
#include <string>

/*
 * Class to process Request Messages
 */
class RequestMessage
{
public:
   char* device_file = new char();
   RequestMessage(const XrdSsiEntity &client, const RmcdServiceProvider *service) {
      using namespace XrdSsiPb;
      Log::Msg(Log::DEBUG, LOG_SUFFIX, "RequestMessage() constructor: request received from client ",
               client.name, '@', client.host);
   }

   /*!
    * Process a Notification request or an Admin command request
    *
    * @param[in]     request
    * @param[out]    response        Response protocol buffer
    * @param[out]    stream          Reference to Response stream pointer
    */
   void process(const rmc_test::Request &request, rmc_test::Response &response, std::string &data_buffer, XrdSsiStream* &stream) {
         
      using namespace XrdSsiPb;
      using namespace rmc_test;
      if (request.has_mount()) {
         std::stringstream message;
         response.set_message_txt(message.str());
         response.set_type(Response::RSP_SUCCESS);
         /* get robot geometry */
         int c;
	 robot_info m_robot_info;
         
         const int max_nb_attempts = 3;
         int attempt_nb = 1;
         for(attempt_nb = 1; attempt_nb <= max_nb_attempts; attempt_nb++) {
         std::cout<<"The device file before the get_geometry is "<<device_file<<std::endl;
         c = smc_get_geometry (-1,
                device_file,              ///dev/sgx for example
                &m_robot_info);
         if (c==0){
            message << "Got geometry of tape library"<< std::endl;
         }
    	 std::string driverStr = std::to_string (request.mount().drvord());
         int n = (request.mount().vid()).length();  
         // declaring character array 
         char *ch_array = new char[n+1]; 
           
         // copying the contents of the string to char array 
         strcpy(ch_array, (request.mount().vid()).c_str());  
                             
         c = smc_mount (-1,-1, device_file, &m_robot_info, 
                request.mount().drvord(), ch_array,  0);    
         }      
      } else if (request.has_dismount()) {
         std::stringstream message;
         response.set_message_txt(message.str());
         response.set_type(Response::RSP_SUCCESS);

         /* get robot geometry */
         int c;
	 robot_info m_robot_info;
         const int max_nb_attempts = 3;
         int attempt_nb = 1;
         for(attempt_nb = 1; attempt_nb <= max_nb_attempts; attempt_nb++) {
            c = smc_get_geometry (-1,
               device_file, &m_robot_info);
            if (c==0){
               message << "Got geometry of tape library"<< std::endl;
            }
            std::string driverStr = std::to_string (request.dismount().drvord());
            int n = (request.dismount().vid()).length();  
      
            /* declaring character array */
            char *ch_array = new char[n+1]; 
               
            /* copying the contents of the string to char array */
            strcpy(ch_array, (request.dismount().vid()).c_str());  
                                
            c = smc_dismount (-1,-1, device_file, &m_robot_info, 
                   request.dismount().drvord(), ch_array);
         }
      }
   }

private:
  
    // Set reply header in metadata
  
   void set_header(rmc_test::Response &response) {
      const char* const TEXT_RED    = "\x1b[31;1m";
      const char* const TEXT_NORMAL = "\x1b[0m\n";

      std::stringstream header;

      header << TEXT_RED << "Count "
                         << "Int64 Value "
                         << "Double Value "
                         << "Bool  String Value" << TEXT_NORMAL;

      response.set_message_txt(header.str());
   }

   static constexpr const char* const LOG_SUFFIX = "Pb::RequestMessage";    //!< Identifier for log messages
};



/*
 * Implementation of XRootD SSI subclasses
 */
namespace XrdSsiPb {

/*
 * Convert a framework exception into a Response
 */

template<>
void ExceptionHandler<rmc_test::Response, PbException>::operator()(rmc_test::Response &response, const PbException &ex)
{
   response.set_type(rmc_test::Response::RSP_ERR_PROTOBUF);
   response.set_message_txt(ex.what());
}



/*
 * Process the Notification Request
 */
template <>
void RequestProc<rmc_test::Request, rmc_test::Response, rmc_test::Alert>::ExecuteAction()
{
   try {
      // Perform a capability query on the XrdSsiProviderServer object: it must be a RmcdServiceProvider

      RmcdServiceProvider *rmc_test_service_ptr;
     
      if(!(rmc_test_service_ptr = dynamic_cast<RmcdServiceProvider*>(XrdSsiProviderServer)))
      {
         throw std::runtime_error("XRootD Service is not the Test Service");
      }
      RequestMessage request_msg(*(m_resource.client), rmc_test_service_ptr);
      strcpy(request_msg.device_file,((rmc_test_service_ptr->scsi_device).c_str()));   //get the filename to mount/dismount from the configuration file
      request_msg.process(m_request, m_metadata, m_response_str, m_response_stream_ptr);
   } catch(PbException &ex) {
      m_metadata.set_type(rmc_test::Response::RSP_ERR_PROTOBUF);
      m_metadata.set_message_txt(ex.what());
   } catch(std::exception &ex) {
      // Serialize and send a log message

      rmc_test::Alert alert_msg;
      alert_msg.set_message_txt("Something bad happened");
      Alert(alert_msg);

      // Send the metadata response

      m_metadata.set_type(rmc_test::Response::RSP_ERR_SERVER);
      m_metadata.set_message_txt(ex.what());
   }
}

} // namespace XrdSsiPb
