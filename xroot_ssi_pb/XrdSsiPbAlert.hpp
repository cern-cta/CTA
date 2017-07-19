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
#include "XrdSsiPbException.hpp"

namespace XrdSsiPb {

/*!
 * Instantiate an Alert message
 */

template<typename AlertType>
class AlertMsg : public XrdSsiRespInfoMsg
{
public:
   AlertMsg(const AlertType &alert) : XrdSsiRespInfoMsg(nullptr, 0)
   {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] AlertMsg() constructor" << std::endl;
#endif

      // Serialize the Alert

      if(!alert.SerializeToString(&alert_str))
      {
         throw PbException("alert.SerializeToString() failed");
      }

      msgBuf = const_cast<char*>(alert_str.c_str());
      msgLen = alert_str.size();
   }

   ~AlertMsg() {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] ~AlertMsg() destructor" << std::endl;
#endif
   }

   void RecycleMsg(bool sent=true) {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] AlertMsg::RecycleMsg()" << std::endl;
      std::cout << "[DEBUG] Alert \"" << alert_str << "\" was " << (sent ? "sent." : "not sent.") << std::endl;
#endif
      delete this;
   }

private:
   std::string alert_str;
};

} // namespace XrdSsiPb

#endif
