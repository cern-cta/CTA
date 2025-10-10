/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <stdexcept>
#include <iostream>
#include <sstream>

#include <XrdSsiPbLog.hpp>

#include <common/dataStructures/FrontendReturnCode.hpp>
#include <common/checksum/ChecksumBlobSerDeser.hpp>
#include "CtaFrontendApi.hpp"



// Define XRootD SSI Alert message callback

namespace XrdSsiPb {

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged by EOS (or directed to the User)
 */

template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert)
{
   std::cout << "AlertCallback():" << std::endl;
   XrdSsiPb::Log::DumpProtobuf(XrdSsiPb::Log::PROTOBUF, &alert);
}

} // namespace XrdSsiPb



//! Usage exception

const std::runtime_error Usage("Usage: cta-wfe-test archive|retrieve|abort|deletearchive [options] [--stderr]");

/*!
 * Fill a Notification message from the command-line parameters
 *
 * @param[out]    notification    The protobuf to fill
 * @param[in]     argc            The number of command-line arguments
 * @param[in]     argv            The command-line arguments
 */

void fillNotification(cta::eos::Notification &notification, int argc, const char *const *const argv)
{
   // First argument must be a valid command specifying which workflow action to execute

   if(argc < 2) throw Usage;

   const std::string wf_command(argv[1]);

   if(wf_command == "archive")
   {
      notification.mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
   }
   else if(wf_command == "retrieve")
   {
      notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
   }
   else if(wf_command == "abort")
   {
      notification.mutable_wf()->set_event(cta::eos::Workflow::ABORT_PREPARE);
   }
   else if(wf_command == "deletearchive")
   {
      notification.mutable_wf()->set_event(cta::eos::Workflow::DELETE);
   }
   else throw Usage;

   // Parse the remaining command-line options

   for(int arg = 2; arg < argc; ++arg)
   {
      const std::string argstr(argv[arg]);

      if(argstr.substr(0,2) != "--" || argc == ++arg) throw std::runtime_error("Arguments must be provided as --key value pairs");

      const std::string argval(argv[arg]);

           if(argstr == "--instance")            notification.mutable_wf()->mutable_instance()->set_name(argval);
      else if(argstr == "--srcurl")              notification.mutable_wf()->mutable_instance()->set_url(argval);

      else if(argstr == "--user")                notification.mutable_cli()->mutable_user()->set_username(argval);
      else if(argstr == "--group")               notification.mutable_cli()->mutable_user()->set_groupname(argval);

      else if(argstr == "--reportURL")           notification.mutable_transport()->set_report_url(argval); // for archive WF
      else if(argstr == "--dsturl")              notification.mutable_transport()->set_dst_url(argval); // for retrieve WF

      else if(argstr == "--diskid")              notification.mutable_file()->set_disk_file_id(argval);
      else if(argstr == "--diskfileowner")       notification.mutable_file()->mutable_owner()->set_uid(std::stoi(argval));
      else if(argstr == "--diskfilegroup")       notification.mutable_file()->mutable_owner()->set_gid(std::stoi(argval));
      else if(argstr == "--size")                notification.mutable_file()->set_size(std::stoi(argval));
      else if(argstr == "--checksumvalue")
      {
         // In principle it's possible to set the full checksum blob with multiple checksums of different
         // types, but this is not currently supported in eos_wfe_stub. It's only possible to set one
         // checksum, which is assumed to be of type ADLER32.
         auto cs = notification.mutable_file()->mutable_csb()->add_cs();
         cs->set_type(cta::common::ChecksumBlob::Checksum::ADLER32);
         cs->set_value(cta::checksum::ChecksumBlob::HexToByteArray(argval));
      }
      else if(argstr == "--diskfilepath")        notification.mutable_file()->set_lpath(argval);
      else if(argstr == "--storageclass")        {
         google::protobuf::MapPair<std::string,std::string> sc("sys.archive.storage_class", argval);
         notification.mutable_file()->mutable_xattr()->insert(sc);
      }
      else if(argstr == "--id")                  {
         google::protobuf::MapPair<std::string,std::string> id("sys.archive.file_id", argval);
         notification.mutable_file()->mutable_xattr()->insert(id);
      }
      else throw std::runtime_error("Unrecognised key " + argstr);
   }
}



/*!
 * Sends a Notification to the CTA XRootD SSI server
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int exceptionThrowingMain(int argc, const char *const *const argv)
{
   // Verify that the Google Protocol Buffer header and linked library versions are compatible
   GOOGLE_PROTOBUF_VERIFY_VERSION;

   cta::xrd::Request request;
   cta::eos::Notification &notification = *(request.mutable_notification());

   // Parse the command line arguments: fill the Notification fields
   fillNotification(notification, argc, argv);

   // Set configuration options
   XrdSsiPb::Config config("/etc/cta/cta-cli.conf", "cta");
   config.set("resource", "/ctafrontend");

   // Allow environment variables to override config file
   config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

   // If XRDDEBUG=1, switch on all logging
   if(getenv("XRDDEBUG")) {
      config.set("log", "all");
   }
   // If fine-grained control over log level is required, use XrdSsiPbLogLevel
   config.getEnv("log", "XrdSsiPbLogLevel");

   // Obtain a Service Provider
   XrdSsiPbServiceType cta_service(config);

   // Send the Request to the Service and get a Response
   cta::xrd::Response response;
   cta_service.Send(request, response, false);

   // Handle responses
   switch(response.type())
   {
      using namespace cta::xrd;

      case Response::RSP_SUCCESS:         std::cout << response.message_txt() << std::endl; break;
      case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
      case Response::RSP_ERR_CTA:         throw std::runtime_error(response.message_txt());
      case Response::RSP_ERR_USER:        throw std::runtime_error(response.message_txt());
      // ... define other response types in the protocol buffer (e.g. user error)
      default:                            throw XrdSsiPb::PbException("Invalid response type.");
   }

   // Delete all global objects allocated by libprotobuf
   google::protobuf::ShutdownProtobufLibrary();

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

