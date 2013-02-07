//         $Id: XrdxCastor2ClientAdmin.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef __XCASTOR2_CLIENT_ADMIN_H__
#define __XCASTOR2_CLIENT_ADMIN_H__

/******************************************************************************/
/*              X r d C a t a l o g C l i e n t A d m i n                     */
/******************************************************************************/

class XrdxCastor2ClientAdmin {
  XrdSysMutex clock;
  XrdClientAdmin* Admin;

public:
  void Lock() {clock.Lock();}
  void UnLock() {clock.UnLock();}
  XrdClientAdmin* GetAdmin() { return Admin;}

  XrdxCastor2ClientAdmin(const char* url) { Admin = new XrdClientAdmin(url);};
  ~XrdxCastor2ClientAdmin() { if (Admin) delete Admin;}
};

#endif
