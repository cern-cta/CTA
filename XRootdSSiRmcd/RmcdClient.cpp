/*!
 * @project        XRootD SSI/Protocol Buffer Interface Project
 * @brief          Command-line rmc_test client for XRootD SSI/Protocol Buffers
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

#include <sstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <common/make_unique.hpp>

#include "XrdSsiPbIStreamBuffer.hpp"
#include "RmcdClient.hpp"

#include <getopt.h>

bool is_metadata_done = false;

// Define XRootD SSI callbacks

namespace XrdSsiPb {

/*
 * Alert callback.
 *
 * Defines how Alert messages should be logged
 */
template<>
void RequestCallback<rmc_test::Alert>::operator()(const rmc_test::Alert &alert)
{
   Log::Msg(Log::INFO, LOG_SUFFIX, "Alert received:");

   // Output message in Json format
   Log::DumpProtobuf(Log::INFO, &alert);
}

} // namespace XrdSsiPb


//
// RmcdClient implementation
//

const std::string DEFAULT_ENDPOINT = "localhost:10956";

// Change console text colour

const char* const TEXT_RED    = "\x1b[31;1m";
const char* const TEXT_NORMAL = "\x1b[0m\n";



// Convert string to bool

bool to_bool(std::string str) {
   std::transform(str.begin(), str.end(), str.begin(), ::tolower);
   if(str == "true") return true;
   if(str == "false") return false;

   throw std::runtime_error(str + " is not a valid boolean value");
}

void RmcdClientCmd::throwUsage(const std::string &error_txt) const
{ 
   std::stringstream help;
   if(error_txt != "") {
      help << m_execname << ": " << error_txt << std::endl << std::endl;
   }
   help << "Usage:\n"
   "\n"
   "cta-xsmc [cmdOptions]\n"
   "Where cmdOptions can be one of those:\n"
   "\n"
   "\tmount -V vid -D drive_ordinal\n"
   "\tdismount\n"
   "\timport\n"
   "\texport\n"
   "\tread_elem  [-N nbelem] [-S starting_slot] [-v]\n"
   "\tfind_cartridge V [-N nbelem] [-V vid] [-v]\n"
   "\tget_geom\n"
   "\n";
   throw std::runtime_error(help.str());
}

void RmcdClientCmd::mountUsage(const std::string &error_txt) const
{
   std::stringstream help;
   if(error_txt != "") {
      help << m_execname << ": " << error_txt << std::endl << std::endl;
   }

   help <<"Usage:\n"
   "\n"
   "  mount [options] -V VID -D DRIVE_SLOT\n"
   "\n"
   "Where:\n"
   "\n"
   "  VID        The VID of the volume to be mounted.\n"
   "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
   "             The format of DRIVE_SLOT is as follows:\n"
   "\n"
   "               ACS:LSM:panel:transport\n"
   "\n"
   "Options:\n"
   "  -h|--help             Print this help message and exit.\n"
   "\n"
   "  -t|--timeout SECONDS  Time to wait for the mount to conclude. SECONDS\n"
   "                        must be an integer value greater than 0.  The\n"
   "                        default value of SECONDS is "
   "\n";
   throw std::runtime_error(help.str());
}

void RmcdClientCmd::dismountUsage(const std::string &error_txt) const
{
   std::stringstream help;

   if(error_txt != "") {
      help << m_execname << ": " << error_txt << std::endl << std::endl;
   }

   help <<"Usage:\n"
   "\n"
   "  dismount [options] -V VID -D DRIVE_SLOT\n"
   "\n"
   "Where:\n"
   "\n"
   "  VID        The VID of the volume to be dismounted.\n"
   "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
   "             The format of DRIVE_SLOT is as follows:\n"
   "\n"
   "               ACS:LSM:panel:transport\n"
   "\n"
   "Options:\n"
   "  -h|--help             Print this help message and exit.\n"
   "\n"
   "  -t|--timeout SECONDS  Time to wait for the dismount to conclude. SECONDS\n"
   "                        must be an integer value greater than 0.  The\n"
   "                        default value of SECONDS is "
   "\n";
   throw std::runtime_error(help.str());
}

RmcdClientCmd::RmcdClientCmd(int argc, char *const *const argv) :
   m_execname(argv[0]),
   m_endpoint(DEFAULT_ENDPOINT)
{
   // Strip path from execname

   size_t p = m_execname.find_last_of('/');
   if(p != std::string::npos) m_execname.erase(0, p+1);

   // Parse the command
   if(argc<2) {
      RmcdClientCmd::throwUsage("Missing command option");
      return; 
   }

 std::string cmdOptions(argv[1]);

   if(cmdOptions == "help") {
      throwUsage("help");
   } 
   else if(cmdOptions == "mount") {
     if(argc==3) {
       std::string cmdh(argv[2]);

       if(cmdh == "help") {
         mountUsage("helping in mount");
         return;
       }
   }
   processMount(argc, argv);
   } else if(cmdOptions == "dismount") {
       if(argc==3) {
         std::string cmdh(argv[2]);

       if(cmdh == "help") {
         dismountUsage("helping in dismount");
         return;
       }
   }
   processDismount(argc, argv);
   } else {
      throwUsage("Unrecognized command: ");
   }

   // Read environment variables

   XrdSsiPb::Config config;

   // If XRDDEBUG=1, switch on all logging
   if(getenv("XRDDEBUG")) {
      config.set("log", "all");
   }

   // XrdSsiPbLogLevel gives more fine-grained control over log level
   config.getEnv("log", "XrdSsiPbLogLevel");

   // Default response timeout
   config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

   // Obtain a Service Provider
   std::string resource("/rmc_test");
   m_rmc_test_service_ptr = cta::make_unique<XrdSsiPbServiceType>(m_endpoint, resource, config);
}

void RmcdClientCmd::processMount(int argc, char *const *const argv) {
     int errflg = 0;
     int drvord = -1;
     int n;
     char vid[7];
     char *dp;
     char c=0;
     static struct option longopts[] = {
       { "help", no_argument, NULL, 'h'},
       { "Vid", required_argument, NULL, 'V'},
       { "Drive_original", required_argument, NULL, 'D' },
       { NULL, 0, NULL, '\0' }
     };
     memset (vid, '\0', sizeof(vid));
     if (argc<6) {
       RmcdClientCmd::mountUsage("less arguments provided");
       return;
     }
     if(*argv[2]!= '-') {
         RmcdClientCmd::mountUsage("Unrecognised command format given\n");
     }

    while ((c = getopt_long(argc, argv, "hV:D:",longopts, NULL))!= -1) {
       switch (c) {
       case 'h':
	 RmcdClientCmd::mountUsage("help");
         break;
       case 'D':       /* drive ordinal */
         {
         drvord = strtol (optarg, &dp, 10);
         if (*dp != '\0' || drvord < 0) {
            errflg++;
         }
            strcpy (dp, optarg);
            printf("drive slot = %s\n",dp);
      	    m_request.mutable_mount()->set_drvord(drvord);
	 break;
         }
       case 'V':       /* vid */
         {
         n = strlen (optarg);
         if (n > 6) {
            errflg++;
         } else {
            strcpy (vid, optarg);
         printf("vid = %s\n",vid);
      	 m_request.mutable_mount()->set_vid(vid);}
         break;
         }
       case '?': /* Unrecognized option */
       default:{
         RmcdClientCmd::mountUsage("Unrecognised commands\n");
	 break;
       }
       }
    }
}

void RmcdClientCmd::processDismount(int argc, char *const *const argv) {
     int errflg = 0;
     int drvord = -1;
     int n;
     char vid[7];
     char *dp;
     char c;
     static struct option longopts[] = {
       { "help", required_argument, NULL, 'h'},
       { "Vid", required_argument, NULL, 'V'},
       { "Drive_original", required_argument, NULL, 'D' },
       { NULL, 0, NULL, '\0' }
     };
     memset (vid, '\0', sizeof(vid));
     if (argc<6) {
       RmcdClientCmd::dismountUsage("less arguments provided");
       return;
     }
     if(*argv[2]!= '-') {
         RmcdClientCmd::dismountUsage("Unrecognised command format given\n");
     }
     while ((c = getopt_long(argc, argv, "hV:D:",longopts, NULL))!= -1) {
       switch (c) {
       case 'h':
	 RmcdClientCmd::dismountUsage("help");
         break;
       case 'D': {      /* drive ordinal */
         drvord = strtol (optarg, &dp, 10);
         if (*dp != '\0' || drvord < 0) {
            errflg++;}
         strcpy (dp, optarg);
         printf("drive slot = %s\n",dp);
      	 m_request.mutable_dismount()->set_drvord(drvord);
         break;
         }
       case 'V': {      /* vid */
         n = strlen (optarg);
         if (n > 6) {
            errflg++;
         } else {
            strcpy (vid, optarg);
         printf("vid = %s\n",vid);
      	 m_request.mutable_dismount()->set_vid(vid);}
         break;
         }
       case '?':
       default:{
        RmcdClientCmd::dismountUsage("Unrecognised command");
	break;
	}
       }
     }
}


void RmcdClientCmd::Send()
{
      // Send the Request to the Service and get a Response
      rmc_test::Response response;
      m_rmc_test_service_ptr->Send(m_request, response);
      // Handle responses
      switch(response.type())
      {
         using namespace rmc_test;
   
         case Response::RSP_SUCCESS:         std::cout << response.message_txt(); break;
         case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
         case Response::RSP_ERR_USER:
         case Response::RSP_ERR_SERVER:      throw std::runtime_error(response.message_txt());
         default:                            throw XrdSsiPb::PbException("Invalid response type.");
      }
      is_metadata_done = true;
}


/*!
 * Start here
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 *
 * @retval 0    Success
 * @retval 1    The client threw an exception
 */
int main(int argc, char **argv)
{
   try {
      // Test uninitialised logging : logging is not available until the rmc_test_service object is
      // instantiated, so this message should be silently ignored
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, "main", "Logging is not initialised");

      // Parse the command line arguments
      RmcdClientCmd cmd(argc, argv);

      // Send the protocol buffer
      cmd.Send();

      // Delete all global objects allocated by libprotobuf
      google::protobuf::ShutdownProtobufLibrary();

      return 0;
   } catch (XrdSsiPb::PbException &ex) {
      std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
   } catch (XrdSsiPb::XrdSsiException &ex) {
      std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
   } catch (std::runtime_error &ex) {
      std::cerr << ex.what() << std::endl;
   } catch (std::exception &ex) {
      std::cerr << "Caught exception: " << ex.what() << std::endl;
   } catch (...) {
      std::cerr << "Caught an unknown exception" << std::endl;
   }

   return 1;
}

