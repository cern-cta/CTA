//          $Id: XrdxCastor2Namespace.cc,v 1.2 2009/06/08 19:15:41 apeters Exp $

#include "XrdxCastor2Fs/XrdxCastor2Fs.hh"
#include "XrdOuc/XrdOucString.hh"

bool 
XrdxCastor2Fs::Map(XrdOucString inmap, XrdOucString &outmap) {
  XrdOucString prefix;
  XrdOucString* nsmap;
  int rpos=STR_NPOS;
  bool found=false;
  while ((rpos = inmap.rfind("/",rpos))!=STR_NPOS) {
    prefix.assign(inmap,0,rpos);
    if ((nsmap = nstable->Find(prefix.c_str()))) {
      // stop with the first match
      found = true;
      break;
    }
    rpos--;
  }

  if (!found) {
    return false;
  }

  outmap.assign(nsmap->c_str(),0, nsmap->length()-1);
  XrdOucString hmap = inmap;
  hmap.erase(prefix,0,prefix.length());
  outmap += hmap;    
  return true;
}
