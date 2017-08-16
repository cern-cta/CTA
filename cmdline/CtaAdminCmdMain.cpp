/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
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

#include <stdexcept>
#include <iostream>
//#include <sstream>

#include "common/dataStructures/FrontendReturnCode.hpp"

#include "XrdSsiPbDebug.hpp"
#include "CtaAdminApi.h"



#if 0
// Define XRootD SSI Alert message callback

namespace XrdSsiPb {

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged by EOS (or directed to the User)
 */

template<>
void RequestCallback<eos::wfe::Alert>::operator()(const eos::wfe::Alert &alert)
{
   std::cout << "AlertCallback():" << std::endl;
   OutputJsonString(std::cout, &alert);
}

} // namespace XrdSsiPb



//! Usage exception

const std::runtime_error Usage("Usage: cta_admin [command] [options]");
#endif



/*!
 * Parse command line and process commands.
 *
 * Valid commands are sent to the CTA Frontend using Google Protocol Buffers 3 + XRootD SSI extensions.
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int exceptionThrowingMain(int argc, const char *const *const argv)
{
#if 0
   // Verify that the Google Protocol Buffer header and linked library versions are compatible

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   eos::wfe::Notification notification;

   // Parse the command line arguments: fill the Notification fields

   bool isStderr, isJson;

   fillNotification(notification, isStderr, isJson, argc, argv);

   std::ostream &myout = isStderr ? std::cerr : std::cout;

   if(isJson)
   {
      XrdSsiPb::OutputJsonString(myout, &notification);
   }

   // Obtain a Service Provider

   std::string host("localhost");
   int port = 10956;
   std::string resource("/ctafrontend");

   XrdSsiPbServiceType cta_service(host, port, resource);

   // Send the Request to the Service and get a Response

   eos::wfe::Response response = cta_service.Send(notification);

   if(isJson)
   {
      XrdSsiPb::OutputJsonString(myout, &response);
   }

   // Handle responses

   switch(response.type())
   {
      using namespace eos::wfe;

      case Response::RSP_SUCCESS:         myout << response.message_txt() << std::endl; break;
      case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
      case Response::RSP_ERR_CTA:         throw std::runtime_error(response.message_txt());
      // ... define other response types in the protocol buffer (e.g. user error)
      default:                            throw XrdSsiPb::PbException("Invalid response type.");
   }

   // Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();
#endif

   return 0;
}



/*!
 * Start here
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int main(int argc, const char **argv)
{
   try {    
      return exceptionThrowingMain(argc, argv);
   } catch (XrdSsiPb::PbException &ex) {
      std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
   } catch (XrdSsiPb::XrdSsiException &ex) {
      std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
   } catch (std::exception &ex) {
      std::cerr << "Caught exception: " << ex.what() << std::endl;
   } catch (...) {
      std::cerr << "Caught an unknown exception" << std::endl;
   }

   return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
}

