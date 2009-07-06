//          $Id: XrdxCastor2Namespace.cc,v 1.3 2009/07/06 08:27:11 apeters Exp $

#include "XrdxCastor2Fs/XrdxCastor2Fs.hh"
#include "XrdOuc/XrdOucString.hh"

bool 
XrdxCastor2Fs::Map(XrdOucString inmap, XrdOucString &outmap) {
  XrdOucString prefix;
  XrdOucString* nsmap;
  int pos = inmap.find('/',1);
  if (pos == STR_NPOS) 
    goto checkroot;

  prefix.assign(inmap, 0, pos);

  // 1st check the namespace with deepness 1
  if (!(nsmap = nstable->Find(prefix.c_str()))) {
    // 2nd check the namespace with deepness 0 e.g. look for a root mapping
  checkroot:
    if (!(nsmap = nstable->Find("/"))) {
      return false;
    } 
    pos =0;
    prefix="/";
  }
  outmap.assign(nsmap->c_str(),0, nsmap->length()-1);
  XrdOucString hmap = inmap;
  hmap.erase(prefix,0,prefix.length());
  outmap += hmap;    
  return true;
}
