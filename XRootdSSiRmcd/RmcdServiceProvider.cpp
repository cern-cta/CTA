/*!
 * @project        XRootD SSI/Protocol Buffer Interface Project
 * @brief          XRootD Service Provider class implementation
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
#include "XrdSsiPbAlert.hpp"
#include "XrdSsiPbService.hpp"
#include <iostream>
#include <string>

#include "protobuf/rmc_test.pb.h"
#include "RmcdServiceProvider.hpp"



/*!
 * Global pointer to the Service Provider object.
 *
 * This must be defined at library load time (i.e. it is a file-level global static symbol). When the
 * shared library is loaded, XRootD initialization fails if the appropriate symbol cannot be found (or
 * it is a null pointer).
 */
XrdSsiProvider *XrdSsiProviderServer = new RmcdServiceProvider;



/*!
 * Initialise the Service Provider
 */
bool RmcdServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
   using namespace XrdSsiPb;

   Log::Say("Called Init(\"", cfgFn, "\",\"", parms, "\")");

   // Extract configuration items from config file
   Config config(cfgFn, "rmc_test");
   auto test_log = config.getOptionList("log");
   if(!test_log.empty()) {
      Log::SetLogLevel(test_log);
   } else {
      Log::SetLogLevel("info");
   }
   
   auto v_scsi_devices = config.getOptionList("cta.xrmcd.smcdev");
   scsi_device = v_scsi_devices[0];
     Log::SetLogLevel(scsi_device);
   if(scsi_device.empty()) {
      Log::SetLogLevel("name of the device_file not specified by the user in the configuration file");
   }
   return true;
}



/*!
 * Instantiate a Service object
 */

XrdSsiService* RmcdServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
   using namespace XrdSsiPb;

   Log::Msg(Log::INFO, LOG_SUFFIX, "Called GetService(", contact, ',', oHold, ')');

   XrdSsiService *ptr = new XrdSsiPb::Service<rmc_test::Request, rmc_test::Response, rmc_test::Alert>;

   return ptr;
}



/*!
 * Query whether a resource exists on a server.
 *
 * @param[in]    rName      The resource name
 * @param[in]    contact    Used by client-initiated queries for a resource at a particular endpoint.
 *                          It is set to NULL for server-initiated queries.
 *
 * @retval    XrdSsiProvider::notPresent    The resource does not exist
 * @retval    XrdSsiProvider::isPresent     The resource exists
 * @retval    XrdSsiProvider::isPending     The resource exists but is not immediately available. (Useful
 *                                          only in clustered environments where the resource may be
 *                                          immediately available on some other node.)
 */

XrdSsiProvider::rStat RmcdServiceProvider::QueryResource(const char *rName, const char *contact)
{
   using namespace XrdSsiPb;

   // We only have one resource
   XrdSsiProvider::rStat resourcePresence = (strcmp(rName, "/rmc_test") == 0) ?
                                            XrdSsiProvider::isPresent : XrdSsiProvider::notPresent;

   Log::Msg(Log::INFO, LOG_SUFFIX, "QueryResource(", rName, "): ", 
             ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent"));

   return resourcePresence;
}

