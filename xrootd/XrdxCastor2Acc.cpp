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
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <fcntl.h>
#include <string.h>
#include <sstream>
/*-----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdxCastor2Acc.hpp"
#include "XrdxCastorTiming.hpp"
/*-----------------------------------------------------------------------------*/
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysDNS.hh"
/*-----------------------------------------------------------------------------*/

XrdSysError AccEroute(0, "xCastor2Acc_");
XrdVERSIONINFO(XrdAccAuthorizeObject, xCastor2Acc);

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
  AccEroute.logger(lp);
  AccEroute.Say("++++++ (c) 2014 CERN/IT-DSS xCastor2Acc v1.0");
  XrdAccAuthorize* acc = (XrdAccAuthorize*) new XrdxCastor2Acc();

  if (!acc)
  {
    AccEroute.Say("------ xCastor2Acc allocation failed!");
    return 0;
  }

  if (!((XrdxCastor2Acc*) acc)->Configure(cfn) ||
      (!((XrdxCastor2Acc*) acc)->Init()))
  {
    AccEroute.Say("------ xCastor2Acc initialization failed!");
    delete acc;
    return 0;
  }
  else
  {
    AccEroute.Say("++++++ xCastor2Acc initialization completed");
    return acc;
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Acc::XrdxCastor2Acc():
  XrdAccAuthorize(),
  LogId(),
  mLogLevel(LOG_INFO),
  mAuthCertfile(""),
  mAuthKeyfile(""),
  mRequireCapability(false),
  mAllowLocal(true)
{ }


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2Acc::~XrdxCastor2Acc()
{ }


//------------------------------------------------------------------------------
// Configure the authorisation plugin
//------------------------------------------------------------------------------
bool
XrdxCastor2Acc::Configure(const char* conf_file)
{
  char* var;
  const char* val;
  int  cfgFD, NoGo = 0;
  XrdOucStream config_stream(&AccEroute, getenv("XRDINSTANCE"));

  if (!conf_file || !*conf_file)
  {
    AccEroute.Emsg("Config", "Configuration file not specified");
  }
  else
  {
    // Try to open the configuration file
    if ((cfgFD = open(conf_file, O_RDONLY, 0)) < 0)
    {
      AccEroute.Emsg("Config", "Error opening config file", conf_file);
      return false;
    }

    config_stream.Attach(cfgFD);

    // Now start parsing records until eof
    while ((var = config_stream.GetMyFirstWord()))
    {
      if (!strncmp(var, "xcastor2.", 9))
      {
        var += 9;

        // Get th public key certificate - only at headnode
        if (!strncmp("publickey", var, 9))
        {
          if (!(val = config_stream.GetWord()))
          {
            AccEroute.Emsg("Config", "publickey argument missing");
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
          if (!(val = config_stream.GetWord()))
          {
            AccEroute.Emsg("Config", "privatekey argument missing");
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
          if (!(val = config_stream.GetWord()))
          {
            AccEroute.Emsg("Config", "Capability arg. missing - "
                           "can be true(1) or false(0)");
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
              AccEroute.Emsg("Config", "Capability arg. invalid - "
                             "can be true(1) or false(0)");
              NoGo = 1;
            }
          }
        }

        // Check for allowlocalhost access
        if (!strcmp("allowlocalhost", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            AccEroute.Emsg("Config", "allowlocalhost arg. missing - "
                           "can be true(1) or false(0)");
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
              AccEroute.Emsg("Config", "allowlocalhost arg. invalid - "
                             "can be true(1) or false(0)");
              NoGo = 1;
            }
          }
        }

        // Get the log level
        if (!strcmp("loglevel", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            AccEroute.Emsg("Config", "argument for debug level invalid set to INFO.");
            mLogLevel = LOG_INFO;
          }
          else
          {
            long int log_level = Logging::GetPriorityByString(val);

            if (log_level == -1)
            {
              // Maybe the log level is specified as an int from 0 to 7
              errno = 0;
              char* end;
              log_level = (int) strtol(val, &end, 10);

              if ((errno == ERANGE && ((log_level == LONG_MIN) || (log_level == LONG_MAX))) ||
                  ((errno != 0) && (log_level == 0)) ||
                  (end == val))
              {
                // There was an error default to LOG_INFO
                log_level = 6;
              }
            }

            mLogLevel = log_level;
          }
        }
      }
    }

    AccEroute.Say("=====> xcastor2.capability: ",
                  (mRequireCapability ? "true" : "false"));
    AccEroute.Say("=====> xcastor2.allowlocalhost: ",
                  (mAllowLocal ? "true" : "false"));

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
          AccEroute.Emsg("Config", "privatekey missing on manager node");
          NoGo = 1;
        }
      }
      else
      {
        if (!mAuthKeyfile.empty())
          mAuthKeyfile.clear();

        if (mAuthCertfile.empty())
        {
          AccEroute.Emsg("Config", "publickey missing on server node");
          NoGo = 1;
        }
      }
    }
  }

  // Get the instance name which for us reflects the type: headnode/diskserver
  char* xrd_name = getenv("XRDNAME");
  AccEroute.Say("=====> XRootD instance type: ", xrd_name);

  if (strcmp(xrd_name, "server") && strcmp(xrd_name, "manager"))
  {
    AccEroute.Emsg("Init", "XRootD unknown instance type: ", xrd_name);
    return false;
  }

  // Enable logging explicitly only at the diskserver as the headnode
  // controls it using the OFS loglevel setting
  if (!strcmp(xrd_name, "server"))
  {
    std::ostringstream unit;
    unit << "acc@" <<  XrdSysDNS::getHostName() << ":1095";
    Logging::Init();
    Logging::SetLogPriority(mLogLevel);
    Logging::SetUnit(unit.str().c_str());
    xcastor_info("acc logging configured");
  }

  if (NoGo)
    return false;

  return true;
}


//------------------------------------------------------------------------------
// Initalise the plugin - get the public/private keys for signing
//------------------------------------------------------------------------------
bool
XrdxCastor2Acc::Init()
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
XrdxCastor2Acc::SignBase64(unsigned char* input,
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
XrdxCastor2Acc::VerifyUnbase64(const char* data,
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

  if (mLogLevel == LOG_DEBUG)
    verifytiming.Print();

  if (err != 1)
    return false;

  return true;
}


//------------------------------------------------------------------------------
// Decode opaque information
//------------------------------------------------------------------------------
bool
XrdxCastor2Acc::Decode(const char* opaque, AuthzInfo& authz)
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

    if (token.beginswith("castor2fs.pool="))
    {
      authz.pool = (token.c_str() + 15);
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

  return true;
}


//------------------------------------------------------------------------------
// Build the auhorization token from the information held in the AuthzInfo
// structure and sign all this with the private key of the server.
//------------------------------------------------------------------------------
std::string
XrdxCastor2Acc::GetOpaqueAcc(AuthzInfo& authz, bool doSign)
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

  // Build the opaque information appended to the URL
  std::ostringstream sstr;
  if (authz.sfn.size() > 0) sstr << "castor2fs.sfn=" << authz.sfn  << "&";
  if (authz.pfn1.size() > 0) sstr << "castor2fs.pfn1=" << authz.pfn1 << "&";
  if (authz.pool.size() > 0) sstr << "castor2fs.pool=" << authz.pool << "&";
  if (authz.pfn2.size() > 0) sstr << "castor2fs.pfn2=" << authz.pfn2 << "&";
  if (authz.id.size() > 0) sstr << "castor2fs.id=" << authz.id << "&";
  if (authz.client_sec_uid.size() > 0)
    sstr << "castor2fs.client_sec_uid=" << authz.client_sec_uid << "&";
  if (authz.client_sec_gid.size() > 0)
    sstr << "castor2fs.client_sec_gid=" << authz.client_sec_gid << "&";
  sstr << "castor2fs.accessop=" << authz.accessop << "&";
  sstr << "castor2fs.exptime=" << (int)authz.exptime << "&";
  sstr << "castor2fs.signature=" << authz.signature << "&";
  if (authz.manager.size() > 0) sstr << "castor2fs.manager=" << authz.manager << "&";

  xcastor_debug("opaque_acc=%s", sstr.str().c_str());
  return sstr.str();
}


//------------------------------------------------------------------------------
// Build the autorization token used for signing
//------------------------------------------------------------------------------
std::string
XrdxCastor2Acc::BuildToken(const AuthzInfo& authz)
{
  std::ostringstream sstr;
  sstr << authz.sfn
       << authz.pfn1
       << authz.pool
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
XrdxCastor2Acc::Access(const XrdSecEntity* Entity,
                             const char* path,
                             const Access_Operation oper,
                             XrdOucEnv* Env)
{
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
    AccEroute.Emsg("Access", "No access opaque information for path", path);
    return XrdAccPriv_None;
  }

  int envlen = 0;
  const char* opaque = Env->Env(envlen);

  if (!opaque)
  {
    AccEroute.Emsg("Access", "No access opaque information for path", path);
    return XrdAccPriv_None;
  }

  xcastor_debug("path=%s, operation=%i, env=%s", path, oper, opaque);
  time_t now = time(NULL);

  // This is not nice, but ROOT puts a ? into the opaque string,
  // if there is a user opaque info
  XrdOucString tmp_opaque = opaque;
  while (tmp_opaque.replace("?", "&")) { };
  while (tmp_opaque.replace("&&", "&")) { };
  opaque = tmp_opaque.c_str();

  XrdSysMutexHelper lock_decode(mDecodeMutex);

  // Decode the authz information from the opaque info
  if (mRequireCapability)
  {
    XrdxCastor2Acc::AuthzInfo authz;

    if (!Decode(opaque, authz))
    {
      AccEroute.Emsg("Access", "Unable to decode access token for sfn=", path);
      return XrdAccPriv_None;
    }

    // Build the token from the received information
    std::string ref_token = BuildToken(authz);

    if (ref_token.empty())
    {
      AccEroute.Emsg("Access", "Empty reference token for path", path);
      return XrdAccPriv_None;
    }

    // Verify the signature of authz information
    if ((!VerifyUnbase64(ref_token.c_str(),
                         (unsigned char*)authz.signature.c_str(), path)))
    {
      AccEroute.Emsg("Access", "Verify signature failed for path", path);
      return XrdAccPriv_None;
    }

    // Check that path in token and request are identical
    if (strcmp(path, authz.pfn1.c_str()) && strcmp(path, authz.pfn2.c_str()))
    {
      // If it does not fit, check that it is a replica location
      if (authz.pfn2.find(authz.pfn1) == std::string::npos)
      {
        AccEroute.Emsg("Access", "Signature was not provided for this path", path);
        return XrdAccPriv_None;
      }
    }

    // Check validity time in authz
    if (authz.exptime < now)
    {
      AccEroute.Emsg("Access", "Signature expired for path", path);
      return XrdAccPriv_None;
    }
  }

  return XrdAccPriv_All;
}
