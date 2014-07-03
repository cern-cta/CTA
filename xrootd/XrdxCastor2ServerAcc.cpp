/*******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * @author castor-dev@cern.ch
 * 
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <fcntl.h>
#include <string.h>
#include <sstream>
/*-----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdxCastor2ServerAcc.hpp"
#include "XrdxCastorTiming.hpp"
/*-----------------------------------------------------------------------------*/
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysDNS.hh"
/*-----------------------------------------------------------------------------*/

XrdSysError TkEroute(0, "xCastorServerAcc");
XrdVERSIONINFO(XrdAccAuthorizeObject, xCastor2ServAcc);

//------------------------------------------------------------------------------
// XrdAccAuthorizeObject() is called to obtain an instance of the auth object
// that will be used for all subsequent authorization decisions. If it returns
// a null pointer; initialization fails and the program exits. The args are:
//
// lp    -> XrdSysLogger to be tied to an XrdSysError object for messages
// cfn   -> The name of the configuration file
// parm  -> Parameters specified on the authlib directive. If none it is zero.
//
//------------------------------------------------------------------------------
extern "C" XrdAccAuthorize* XrdAccAuthorizeObject(XrdSysLogger* lp,
                                                  const char* cfn,
                                                  const char* parm)
{
  TkEroute.SetPrefix("XrdxCastor2ServerAcc::");
  TkEroute.logger(lp);
  TkEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
               "xCastor2ServerAcc (xCastor File System) v 1.0");
  XrdAccAuthorize* acc = (XrdAccAuthorize*) new XrdxCastor2ServerAcc();

  if (!acc)
  {
    TkEroute.Say("------ xCastor2ServerAcc Allocation Failed!");
    return 0;
  }

  if (!(((XrdxCastor2ServerAcc*) acc)->Configure(cfn)) ||
      (!(((XrdxCastor2ServerAcc*) acc)->Init())))
  {
    TkEroute.Say("------ xCastor2ServerAcc initialization failed!");
    delete acc;
    return 0;
  }
  else
  {
    TkEroute.Say("------ xCastor2ServerAcc initialization completed");
    return acc;
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2ServerAcc::XrdxCastor2ServerAcc():
  XrdAccAuthorize(),
  LogId(),
  mAuthCertfile(""),
  mAuthKeyfile(""),
  mRequireCapability(false),
  mAllowLocal(true)
{
  Logging::Init();
  Logging::SetLogPriority(LOG_INFO);
  xcastor_info("logging configured");
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2ServerAcc::~XrdxCastor2ServerAcc()
{ }


//------------------------------------------------------------------------------
// Configure the authorisation plugin
//------------------------------------------------------------------------------
bool
XrdxCastor2ServerAcc::Configure(const char* conf_file)
{
  char* var;
  const char* val;
  int  cfgFD, NoGo = 0;
  XrdOucStream Config(&TkEroute, getenv("XRDINSTANCE"));

  if (!conf_file || !*conf_file)
  {
    xcastor_err("configuration file not specified");
  }
  else
  {
    // Try to open the configuration file
    if ((cfgFD = open(conf_file, O_RDONLY, 0)) < 0)
    {
      xcastor_err("error while opening config file:%s", conf_file);
      return false;
    }

    Config.Attach(cfgFD);

    // Now start parsing records until eof
    while ((var = Config.GetMyFirstWord()))
    {
      if (!strncmp(var, "xcastor2.", 9))
      {
        var += 9;

        // Get th public key certificate - only at headnode
        if (!strncmp("publickey", var, 9))
        {
          if (!(val = Config.GetWord()))
          {
            xcastor_err("publickey arg. missing");
            NoGo = 1;
          }
          else
          {
            mAuthCertfile = val;
          }
        }

        // Get the private key certificate - only on diskservers
        if (!strncmp("privatekey", var, 10))
        {
          if (!(val = Config.GetWord()))
          {
            xcastor_err("privatekey arg. missing");
            NoGo = 1;
          }
          else
          {
            mAuthKeyfile = val;
          }
        }

        // Check for capability tag
        if (!strncmp("capability", var, 10))
        {
          if (!(val = Config.GetWord()))
          {
            xcastor_err("capability arg. missing. Can be true(1) or false(0)");
            NoGo = 1;
          }
          else
          {
            if (!strcmp(val, "true") || !strcmp(val, "1"))
              mRequireCapability = true;
            else if (!(strcmp(val, "false")) || !(strcmp(val, "0")))
              mRequireCapability = false;
            else
            {
              xcastor_err("capability arg. invalid. Can be true(1) or false(0)");
              NoGo = 1;
            }
          }
        }

        // Check for allowlocalhost access
        if (!strcmp("allowlocalhost", var))
        {
          if (!(val = Config.GetWord()))
          {
            xcastor_err("allowlocalhost arg. missing. Can be <true>/1 or <false>/0");
            NoGo = 1;
          }
          else
          {
            if (!strcmp(val, "true") || !strcmp(val, "1"))
              mAllowLocal = true;
            else if (!strcmp(val, "false") || !strcmp(val, "0"))
              mAllowLocal = false;
            else
            {
              xcastor_err("allowlocalhost arg. invalid. Can be true(1) or false(0)");
              NoGo = 1;
            }
          }
        }
      }
    }

    xcastor_info("=====> xcastor2.capability: %i" , mRequireCapability);
    xcastor_info("=====> xcastor2.allowlocalhost: %i", mAllowLocal);

    // Get XRootD server type
    const char* tp;
    int isMan = ((tp = getenv("XRDREDIRECT")) && !strncmp(tp, "R", 1));

    if (mRequireCapability)
    {
      if (isMan)
      {
        if (!mAuthCertfile.empty())
          mAuthCertfile.clear();

        if (mAuthKeyfile.empty())
        {
          xcastor_err("privatkey mising on manager host");
          NoGo = 1;
        }
      }
      else
      {
        if (!mAuthKeyfile.empty())
          mAuthKeyfile.clear();

        if (mAuthCertfile.empty())
        {
          xcastor_err("publickey missing on server host");
          NoGo = 1;
        }
      }
    }
  }

  if (NoGo)
    return false;

  return true;
}


//------------------------------------------------------------------------------
// Initalise the plugin - get the public/private keys for signing
//------------------------------------------------------------------------------
bool
XrdxCastor2ServerAcc::Init()
{
  // Read in the public key
  if (!mAuthCertfile.empty())
  {
    FILE* fpcert = fopen(mAuthCertfile.c_str(), "r");

    if (fpcert == NULL)
    {
      xcastor_err("error opening public cert. file:%s" , mAuthCertfile.c_str());
      return false;
    }

    // Get the public key
    X509* x509public = PEM_read_X509(fpcert, NULL, NULL, NULL);
    fclose(fpcert);

    if (x509public == NULL)
    {
      xcastor_err("error accessing X509 in file:%s", mAuthCertfile.c_str());
      return false;
    }
    else
    {
      mPublicKey = X509_get_pubkey(x509public);
      
      if (mPublicKey == NULL)
      {
        xcastor_err("no public key in file:%s", mAuthCertfile.c_str());
        return false;
      }
    }
  }

  // Get the private key
  if (!mAuthKeyfile.empty())
  {
    FILE* fpkey = fopen(mAuthKeyfile.c_str(), "r");

    if (fpkey == NULL)
    {
      xcastor_err("error opening private cert. file:%s", mAuthKeyfile.c_str());
      return false;
    }
    
    // Get private key
    mPrivateKey = PEM_read_PrivateKey(fpkey, NULL, NULL, NULL);
    fclose(fpkey);
    
    if (mPrivateKey == NULL)
    {
      xcastor_err("error accessing private key in file:%s", mAuthKeyfile.c_str());
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Signature with base64 encoding ( hash - sign - encode base64 )
//------------------------------------------------------------------------------
bool
XrdxCastor2ServerAcc::SignBase64(unsigned char* input,
                                 int inputlen,
                                 std::string& sb64,
                                 int& sb64len)
{
  int err;
  unsigned int sig_len;
  unsigned char sig_buf[16384];
  char signed_signature_buff[16384];
  xcastor::Timing signtiming("SignBase64");
  TIMING("START", &signtiming);
  EVP_MD_CTX md_ctx;
  EVP_MD_CTX_init(&md_ctx);
  BIO* bmem, *b64;
  BUF_MEM* bptr;
  sb64 = "";

  // Use the envelope api to create and encode the hash value. First the implementation
  // of the digest type is sent and then the input data is hashed into the context
  EVP_SignInit(&md_ctx, EVP_sha1());
  EVP_SignUpdate(&md_ctx, input, inputlen);
  sig_len = sizeof(sig_buf);
  TIMING("EVPINIT/UPDATE", &signtiming);

  mEncodeMutex.Lock(); // -->
  EVP_PKEY* usekey = mPrivateKey;
  err = EVP_SignFinal(&md_ctx, sig_buf, &sig_len, usekey);
  mEncodeMutex.UnLock(); // <--
  TIMING("EVPSIGNFINAL", &signtiming);

  if (!err)
  {
    xcastor_err("unable to sign hash value, check private key");
    return false;
  }

  EVP_MD_CTX_cleanup(&md_ctx);
  TIMING("EVPCLEANUP", &signtiming);

  // Base64 encode
  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, sig_buf, sig_len);
  err = BIO_flush(b64);
  BIO_get_mem_ptr(b64, &bptr);
  TIMING("BASE64", &signtiming);
  char* buff = bptr->data;
  int cnt = 0;

  // Remove the backslash from the signature buffer
  for (unsigned int i = 0; i <= (bptr->length - 1); i++)
  {
    if (buff[i] != '\n')
    {
      signed_signature_buff[cnt] = buff[i];
      cnt++;
    }
  }

  signed_signature_buff[cnt] = 0;
  sb64len = cnt;
  sb64 = signed_signature_buff;
  BIO_free(bmem);
  BIO_free(b64);
  TIMING("BIOFREE", &signtiming);
  //signtiming.Print();
  return true;
}


//------------------------------------------------------------------------------
// Verify if the data buffer matches the signature provided in base64buffer
//------------------------------------------------------------------------------
bool
XrdxCastor2ServerAcc::VerifyUnbase64(const char* data,
                                     unsigned char* base64buffer,
                                     const char* path)
{
  xcastor::Timing verifytiming("VerifyUb64");
  TIMING("START", &verifytiming);
  int sig_len = 0;
  char sig_buf[16384];
  EVP_MD_CTX md_ctx;

  // Base64 decode
  BIO* b64, *bmem;
  int cpcnt = 0;
  char modinput[16384];
  int modlength;
  int inputlen = strlen((const char*)base64buffer);
  unsigned char* input  = base64buffer;
    
  for (int i = 0; i < (inputlen + 1); i++)
  {
    // Add a '\n' every 64 characters which have been removed to be
    // compliant with the HTTP URL syntax
    if (i && (i % 64 == 0))
    {
      modinput[cpcnt] = '\n';
      cpcnt++;
    }
    
    modinput[cpcnt] = input[i];
    cpcnt++;
  }
  
  modinput[cpcnt] = 0;
  modlength = cpcnt - 1;
  b64 = BIO_new(BIO_f_base64());
  
  if (!b64) 
  {
    xcastor_err("unable to allocate new BIO");
    return false;
  }
  
  bmem = BIO_new_mem_buf(modinput, modlength);
  
  if (!bmem)
  {
    xcastor_err("unable to allocate new mem buf");
    BIO_free_all(b64);
    return false;
  }
  
  bmem = BIO_push(b64, bmem);
  sig_len = BIO_read(bmem, sig_buf, modlength);
  BIO_free_all(bmem);
  
  if (sig_len <= 0) 
  {
    xcastor_err("error while decoding base64 message for path=%s", path);
    return false;
  }

  TIMING("UNBASE64", &verifytiming);
  // Verify the signature using the public key
  // printf("Verify %d %s %d %d\n",strlen((char*)data),data,publickey, sig_len);
  EVP_VerifyInit(&md_ctx, EVP_sha1());
  EVP_VerifyUpdate(&md_ctx, data, strlen((char*)data));
  int err = EVP_VerifyFinal(&md_ctx, ((unsigned char*)(sig_buf)), sig_len, mPublicKey);
  EVP_MD_CTX_cleanup(&md_ctx);
  TIMING("EVPVERIFY", &verifytiming);
  verifytiming.Print();

  if (err != 1)
    return false;

  return true;
}


//------------------------------------------------------------------------------
// Decode opaque information
//------------------------------------------------------------------------------
bool
XrdxCastor2ServerAcc::Decode(const char* opaque, AuthzInfo& authz)
{
  if (!opaque) return false;

  int ntoken = 0;
  const char* stoken;
  XrdOucString tmp_str = opaque;

  // Convert the '&' seperated tokens into '\n' seperated tokens for parsing
  tmp_str.replace("&", "\n");
  XrdOucTokenizer authztokens((char*)tmp_str.c_str());
  
  while ((stoken = authztokens.GetLine()))
  {
    XrdOucString token = stoken;

    // Check the existance of the castor2fs tokens in the opaque info
    if (token.beginswith("castor2fs.sfn="))
    {
      authz.sfn = (token.c_str() + 14);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.pfn1="))
    {
      authz.pfn1 = (token.c_str() + 15);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.pfn2="))
    {
      authz.pfn2 = (token.c_str() + 15);
      ntoken++;
      continue;
    }
    
    if (token.beginswith("castor2fs.id="))
    {
      authz.id = (token.c_str() + 13);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.client_sec_uid="))
    {
      authz.client_sec_uid = (token.c_str() + 25);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.client_sec_gid="))
    {
      authz.client_sec_gid = (token.c_str() + 25);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.accessop="))
    {
      authz.accessop = atoi(token.c_str() + 19);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.exptime="))
    {
      authz.exptime = (time_t) strtol(token.c_str() + 18, 0, 10);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.signature="))
    {
      authz.signature = (token.c_str() + 20);
      ntoken++;
      continue;
    }

    if (token.beginswith("castor2fs.manager="))
    {
      authz.manager = (token.c_str() + 18);
      ntoken++;
      continue;
    }
  }

  // We expect 10 opaque castor2fs tokens to be present
  if (ntoken != 10)
  {
    xcastor_err("wrong number of tokens:%i", ntoken);
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Build the auhorization token from the information held in the AuthzInfo
// structure and sign all this with the private key of the server.
//------------------------------------------------------------------------------
std::string 
XrdxCastor2ServerAcc::GetOpaqueAcc(AuthzInfo& authz, bool doSign)
{
  // Build authorization token
  std::string token = BuildToken(authz);
  
  if (token.empty())
  {
    xcastor_err("authorization token is empty - nothing to sign");
    return "";
  }

  // Sign hash and sign the token if requested
  if (doSign)
  {
    std::string sb64 = "";
    int sb64len = 0;

    if (!SignBase64((unsigned char*) token.c_str(), token.length(), sb64, sb64len))
    {
      xcastor_err("failed to sign authorization token");
      return "";
    }

    authz.signature = (char*) sb64.c_str();
  }

  // Build the opauqe information appended to the URL
  std::ostringstream sstr;
  sstr << "castor2fs.sfn=" << authz.sfn  << "&"
       << "castor2fs.pfn1=" << authz.pfn1 << "&"
       << "castor2fs.pfn2=" << authz.pfn2 << "&"
       << "castor2fs.id=" << authz.id << "&"
       << "castor2fs.client_sec_uid=" << authz.client_sec_uid << "&"
       << "castor2fs.client_sec_gid=" << authz.client_sec_gid << "&"
       << "castor2fs.accessop=" << authz.accessop << "&"
       << "castor2fs.exptime=" << (int)authz.exptime << "&"
       << "castor2fs.signature=" << authz.signature << "&"
       << "castor2fs.manager=" << authz.manager << "&";
  return sstr.str();
}


//------------------------------------------------------------------------------
// Build the autorization token used for signing
//------------------------------------------------------------------------------
std::string 
XrdxCastor2ServerAcc::BuildToken(const AuthzInfo& authz)
{
  std::ostringstream sstr;
  sstr << authz.sfn
       << authz.pfn1 
       << authz.pfn2
       << authz.id
       << authz.client_sec_uid 
       << authz.client_sec_gid
       << (int)authz.accessop 
       << (int)authz.exptime 
       << authz.manager;
  return sstr.str();
}


//------------------------------------------------------------------------------
// Indicates whether or not the user/host is permitted access to the path for
// the specified operation
//------------------------------------------------------------------------------
XrdAccPrivs
XrdxCastor2ServerAcc::Access(const XrdSecEntity* Entity,
                             const char* path,
                             const Access_Operation oper,
                             XrdOucEnv* Env)
{
  xcastor_debug("path=%s, operation=%i", path, oper);
  
  // We take care in XrdxCastorOfs::open that a user cannot give a fake 
  // opaque to get all permissions!
  if (Env && Env->Get("castor2ofsproc") &&
      (strncmp(Env->Get("castor2ofsproc"), "true", 4) == 0))
  {
    return XrdAccPriv_All;
  }

  // Check for localhost host connection
  if (mAllowLocal)
  {
    XrdOucString chost = Entity->host;

    if ((chost == "localhost" ||
         (chost == "localhost.localdomain") ||
         (chost == "127.0.0.1")))
    {
      return XrdAccPriv_All;
    }
  }

  if (!Env)
  {
    xcastor_err("no opaque information for path=%s", path);
    TkEroute.Emsg("Access", EIO, "no opaque information for path=", path);
    return XrdAccPriv_None;
  }
  
  int envlen = 0;
  char* opaque = Env->Env(envlen);

  
  if (!opaque)
  {
    TkEroute.Emsg("Access", EIO, "no opaque information for sfn=", path);
    return XrdAccPriv_None;
  }

  xcastor_debug("path=%s, operation=%i, env=%s", path, oper, opaque);
  time_t now = time(NULL);
  
  // This is not nice, but ROOT puts a ? into the opaque string,
  // if there is a user opaque info
  for (unsigned int i = 0; i < strlen(opaque); i++)
  {
    if (opaque[i] == '?')
      opaque[i] = '&';
  }

  // TODO: maybe this lock can be removed
  XrdSysMutexHelper lock_decode(mDecodeMutex);

  // Decode the authz information from the opaque info
  if (mRequireCapability)
  {
    XrdxCastor2ServerAcc::AuthzInfo authz;

    if (!Decode(opaque, authz))
    {
      TkEroute.Emsg("Access", EACCES, "decode access token for sfn=", path);
      return XrdAccPriv_None;
    }
    
    // Build the token from the received information
    std::string ref_token = BuildToken(authz);
    
    if (ref_token.empty())
    {
      TkEroute.Emsg("Access", EACCES, "build reference token for sfn=", path);
      return XrdAccPriv_None;
    }

    // Verify the signature of authz information
    if ((!VerifyUnbase64(ref_token.c_str(), 
                         (unsigned char*)authz.signature.c_str(), path)))
    {
      TkEroute.Emsg("Access", EACCES, "verify signature in request sfn=", path);
      return XrdAccPriv_None;
    }

    // Check that path in token and request are identical
    if (strcmp(path, authz.pfn1.c_str()) && strcmp(path, authz.pfn2.c_str()))
    {
      // If it does not fit, check that it is a replica location
      if (authz.pfn2.find(authz.pfn1) == std::string::npos)
      {
        TkEroute.Emsg("Access", EACCES, "give access - the signature was not "
                      "provided for this path!");
        return XrdAccPriv_None;
      }
    }

    // Check validity time in authz
    if (authz.exptime < now)
    {
      TkEroute.Emsg("Access", EACCES, "give access - the signature has expired already!");
      return XrdAccPriv_None;
    }
  }

  return XrdAccPriv_All;
}

