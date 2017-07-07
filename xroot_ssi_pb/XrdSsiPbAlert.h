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
