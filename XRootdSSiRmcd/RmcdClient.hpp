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

#pragma once

#include "RmcdApi.hpp"

class RmcdClientCmd
{
public:
   RmcdClientCmd(int argc, char *const *const argv);

   /*!
    * Send the protocol buffer across the XRootD SSI transport
    */
   void Send();
   /*!
    * Throw an exception with usage help
    */
   void  mountUsage(const std::string &error_txt = "") const;
   void  dismountUsage(const std::string &error_txt = "") const;
   void  throwUsage(const std::string &error_txt = "") const;
   void  processMount(int, char * const *const);
   void  processDismount(int, char * const *const);

private:

   /*
    * Member variables
    */
   std::unique_ptr<XrdSsiPbServiceType> m_rmc_test_service_ptr;    //!< Pointer to Service object
   std::string                          m_execname;            //!< Executable name of this program
   std::string                          m_endpoint;            //!< hostname:port of XRootD server
   rmc_test::Request                    m_request;             //!< Protocol Buffer for the command to send to the server
};

