#ifndef __CS2_OFS_RATE_LIMIT_H__
#define __CS2_OFS_RATE_LIMIT_H__

#include "XrdSys/XrdSysError.hh"

extern void *XrdCS2OfsRateLimitStart(void *pp);

class XrdCS2OfsRateLimit {
private:
  XrdSysError*          eDest;

public:
  void                   Limit();
  bool                   Init(XrdSysError &ep);
  XrdCS2OfsRateLimit() {};  
  virtual ~XrdCS2OfsRateLimit() {};
};

#endif
