//         $Id: XrdxCastor2FsSecurity.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef ___XrdxCastor2FsSECURITY_H___
#define ___XrdxCastor2FsSECURITY_H___


#include "XrdAcc/XrdAccAuthorize.hh"

#define AUTHORIZE(usr, env, optype, action, pathp, edata) \
    if (usr && XrdxCastor2FS->Authorization \
    &&  !XrdxCastor2FS->Authorization->Access(usr, pathp, optype, env)) \
       {XrdxCastor2FS->Emsg(epname, edata, EACCES, action, pathp); return SFS_ERROR;}

#define AUTHORIZE2(usr,edata,opt1,act1,path1,env1,opt2,act2,path2,env2) \
       {AUTHORIZE(usr, env1, opt1, act1, path1, edata); \
        AUTHORIZE(usr, env2, opt2, act2, path2, edata); \
       }

#define OOIDENTENV(usr, env) \
    if (usr) {if (usr->name) env.Put(SEC_USER, usr->name); \
              if (usr->host) env.Put(SEC_HOST, usr->host);}
#endif
