//         $Id: XrdxCastor2OfsRateLimit.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef __XCASTOR2_OFS_RATE_LIMIT_H__
#define __XCASTOR2_OFS_RATE_LIMIT_H__

#include "XrdSys/XrdSysError.hh"

extern void *XrdxCastor2OfsRateLimitStart(void *pp);

class XrdxCastor2OfsRateLimit {
private:
  XrdSysError*          eDest;

public:
  void                   Limit();
  bool                   Init(XrdSysError &ep);
  XrdxCastor2OfsRateLimit() {};  
  virtual ~XrdxCastor2OfsRateLimit() {};
};

#endif
