/*!
 * @project        XRootD SSI/Protocol Buffer Interface Project
 * @brief          XRootD SSI/Google Protocol Buffer bindings for the rmc_test client/server
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

#include "XrdSsiPbServiceClientSide.hpp"                   //!< XRootD SSI/Protocol Buffer Service, client-side bindings
#include "protobuf/rmc_test.pb.h"                                       //!< Auto-generated message types from .proto file

/*!
 * Bind the type of the XrdSsiService to the types defined in the .proto file
 */
typedef XrdSsiPb::ServiceClientSide<rmc_test::Request,     //!< XrdSSi Request message type
                                    rmc_test::Response,    //!< XrdSsi Metadata message type
                                    rmc_test::Data,        //!< XrdSsi Data message type
                                    rmc_test::Alert>       //!< XrdSsi Alert message type
        XrdSsiPbServiceType;

/*!
 * Bind the type of the XrdSsiRequest to the types defined in the .proto file
 */
typedef XrdSsiPb::Request<rmc_test::Request,               //!< XrdSSi Request message type
                          rmc_test::Response,              //!< XrdSsi Metadata message type
                          rmc_test::Data,                  //!< XrdSsi Data message type
                          rmc_test::Alert>                 //!< XrdSsi Alert message type
        XrdSsiPbRequestType;

