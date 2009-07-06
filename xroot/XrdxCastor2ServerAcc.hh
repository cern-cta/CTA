//         $Id: XrdxCastor2ServerAcc.hh,v 1.3 2009/07/06 08:27:11 apeters Exp $

#ifndef __XCASTOR2_SERVER_ACC__
#define __XCASTOR2_SERVER_ACC__

#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdAcc/XrdAccPrivs.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"

#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>


class XrdOucEnv;
class XrdSecEntity;

class XrdxCastor2ServerAcc
{
public:
  struct AuthzInfo {
    char   *sfn;                 /* sfn */
    char   *pfn1;                /* physical mount filename */
    char   *pfn2;                /* physical replica filename, if exists otherwise '-' */
    char   *id;                  /* the client connection id */
    char   *client_sec_uid;      /* the sec identity eg. user name */
    char   *client_sec_gid;      /* the sec identity eg. group name */
    int    accessop;             /* the access operation allowed -> see XrdAcc/XrdAccAuthorize.hh */
    time_t exptime;              /* time when the authorization expires */
    char   *token;               /* the full token */
    char   *signature;           /* signature for 'token' */
    char   *manager;             /* hostname of the managernode */

    AuthzInfo () { sfn = pfn1= pfn2 = id =client_sec_uid =client_sec_gid = token = signature = manager = NULL; accessop=0;exptime=0;}
    void DeleteMembers() {
      if (sfn)  delete sfn;
      if (pfn1) delete pfn1;
      if (pfn2) delete pfn2;
      if (id)   delete id;
      if (client_sec_uid) delete client_sec_uid;
      if (client_sec_gid) delete client_sec_gid;
      if (token)         delete token;
      if (signature)     delete signature;
      if (manager)       delete manager;
    }
  };

  static bool BuildToken(XrdxCastor2ServerAcc::AuthzInfo* authz, XrdOucString &token);
  static bool BuildOpaque(XrdxCastor2ServerAcc::AuthzInfo* authz, XrdOucString &opaque);
  bool        SignBase64(unsigned char *input, int inputlen, XrdOucString& sb64, int& sig64len, const char* keyclass="default");

  /* Access() indicates whether or not the user/host is permitted access to the
     path for the specified operation. The default implementation that is
     statically linked determines privileges by combining user, host, user group, 
     and user/host netgroup privileges. If the operation is AOP_Any, then the 
     actual privileges are returned and the caller may make subsequent tests using 
     Test(). Otherwise, a non-zero value is returned if access is permitted or a 
     zero value is returned is access is to be denied. Other iplementations may
     use other decision making schemes but the return values must mean the same.
     
     Parameters: Entity    -> Authentication information
     path      -> The logical path which is the target of oper
     oper      -> The operation being attempted (see above)
     Env       -> Environmental information at the time of the
     operation as supplied by the path CGI string.
     This is optional and the pointer may be zero.
  */
  
  virtual XrdAccPrivs Access(const XrdSecEntity    *Entity,
			     const char            *path,
			     const Access_Operation oper,
			     XrdOucEnv       *Env=0);
  
  virtual int         Audit(const int              accok,
			    const XrdSecEntity    *Entity,
			    const char            *path,
			    const Access_Operation oper,
			    XrdOucEnv       *Env=0) {return 0;}
  
  // Test() check whether the specified operation is permitted. If permitted it
  // returns a non-zero. Otherwise, zero is returned.
  //
  virtual int         Test(const XrdAccPrivs priv,
			   const Access_Operation oper) { return 0;}

          bool        Init();
          bool        Configure(const char* ConfigFN);

  XrdxCastor2ServerAcc(){auth_keylist=0;}
  
  virtual                  ~XrdxCastor2ServerAcc(); 

private:

  char *auth_certfile;            /* file name of public key for signature verification */
  XrdOucHash<XrdOucString> auth_keyfile;             /* file name of private key for signature creation */
  XrdOucTList* auth_keylist;

  bool VerifyUnbase64(const char* data, unsigned char *base64buffer, const char* path);

  XrdxCastor2ServerAcc::AuthzInfo* Decode(const char* opaque);
  
  char x2c(const char *what);
  void unescape_url(char *url);

  bool RequireCapability;         /* a client has to show up with a capability in the opaque info, if true */
  bool StrictCapability;          /* a client has to show up with a capability but we don't require the authentication ID used in the capability e.g. we can run the disk server without strong authentication! */
  bool AllowLocalhost;            /* a client connecting from localhost does not need authorization [default=yes] */
  bool AllowXfer;                 /* a client coming from a transfer connection does not need authorization [default=yes] */
  
  X509* x509public;
  EVP_PKEY *      publickey;
  XrdOucHash<EVP_PKEY> privatekey;
  XrdSysMutex     decodeLock;
  XrdSysMutex     encodeLock;
};

#endif
