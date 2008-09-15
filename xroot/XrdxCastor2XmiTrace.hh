//         $Id: XrdxCastor2XmiTrace.hh,v 1.1 2008/09/15 10:04:05 apeters Exp $
#ifndef __XCASTOR2_XMI_TRACE_H__
#define __XCASTOR2_XMI_TRACE_H__

#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"

#define TRACE_ALL       0xffff
#define TRACE_Debug     0x0001
#define TRACE_Stage     0x0002
#define TRACE_Defer     0x0004
#define TRACE_Forward   0x0008
#define TRACE_Redirect  0x0010
#define TRACE_Files     0x0020

#include "XrdSys/XrdSysHeaders.hh"
#include "XrdOuc/XrdOucTrace.hh"

#define QTRACE(act) Trace->What & TRACE_ ## act

#define DEBUGR(y) if (Trace->What & TRACE_Debug) \
                  {Trace->Beg(epname, Arg.Ident); cerr <<y; Trace->End();}

#define DEBUG(y) if (Trace->What & TRACE_Debug) TRACEX(y)

#define ZTRACE(x,y) if (Trace->What & TRACE_ ## x) TRACEX(y)

#define TRACER(x,y) if (Trace->What & TRACE_ ## x) \
                       {Trace->Beg(epname, Arg.Ident); cerr <<y; Trace->End();}

#define TRACEX(y) {Trace->Beg(0,epname); cerr <<y; Trace->End();}

#define EPNAME(x) const char *epname = x;
#endif
