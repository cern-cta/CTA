/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Class to manage XRootD SSI alerts
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

#ifndef __XRD_SSI_PB_ALERT_H
#define __XRD_SSI_PB_ALERT_H

#include <XrdSsi/XrdSsiRespInfo.hh>
#include "XrdSsiPbException.h"

template<typename AlertType>
class AlertMsg : public XrdSsiRespInfoMsg
{
public:
   AlertMsg(const AlertType &alert) : XrdSsiRespInfoMsg(nullptr, 0)
   {
      // Serialize the Alert

      if(!alert.SerializeToString(&alert_str))
      {
         throw XrdSsiPbException("alert.SerializeToString() failed");
      }

      msgBuf = const_cast<char*>(alert_str.c_str());
      msgLen = alert_str.size();
   }

   ~AlertMsg() {}

   void RecycleMsg(bool sent=true) { delete this; }

private:
   std::string alert_str;
};

#endif
