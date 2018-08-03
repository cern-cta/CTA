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

#pragma once

#include <XrdSsi/XrdSsiProvider.hh>
#include "protobuf/rmc_test.pb.h"
#include "XrdSsiPbConfig.hpp"



/*!
 * Global pointer to the Service Provider object.
 */
extern XrdSsiProvider *XrdSsiProviderServer;



/*!
 * Instantiates a Service to process client requests.
 */
class RmcdServiceProvider : public XrdSsiProvider
{
public:
   RmcdServiceProvider() {
      // No logging here as we don't set the log level until Init() is called
   }

   virtual ~RmcdServiceProvider() {
      using namespace XrdSsiPb;

      Log::Msg(Log::DEBUG, LOG_SUFFIX, "Called RmcdServiceProvider() destructor");
   }

   /*!
    * Initialize the object.
    *
    * This is always called before any other method.
    */
   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,
             const std::string parms, int argc, char **argv) override;

   /*!
    * Called exactly once after initialisation to obtain an instance of an XrdSsiService object
    */
   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   /*!
    * Determine resource availability. Can be called any time the client asks for the resource status.
    */
   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;
   
   /*!
    * Defines the device file to mount or dismount the volume
    */
   std::string scsi_device;

private:
   static constexpr const char* const LOG_SUFFIX  = "TestStream";    //!< Identifier for log messages
};

