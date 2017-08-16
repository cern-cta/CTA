/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Bind the XRootD SSI transport layer to a set of Google Protocol Buffer definitions
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

#ifndef __EOS_CTA_API_H
#define __EOS_CTA_API_H

#include "XrdSsiPbServiceClientSide.hpp"     //!< XRootD SSI/Protocol Buffer Service, client-side bindings
#include "eos/messages/eos_messages.pb.h"    //!< Auto-generated message types from .proto file

/*!
 * Bind the type of the XrdSsiService to the types defined in the .proto file
 */

typedef XrdSsiPb::ServiceClientSide<eos::wfe::Notification,    //!< XrdSSi Request message type
                                    eos::wfe::Response,        //!< XrdSsi Metadata message type
                                    eos::wfe::Alert>           //!< XrdSsi Alert message type
        XrdSsiPbServiceType;

#endif
