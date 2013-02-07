//          $Id: XrdxCastor2ServerAcc.cc,v 1.2 2009/05/06 14:37:23 apeters Exp $

#include "XrdxCastor2Trace.hh"
#include "XrdxCastor2ServerAcc.hh"
#include "XrdxCastor2Ofs.hh"
#include "XrdxCastor2Timing.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysError.hh"
#include <string.h>

XrdSysError TkEroute(0,"xCastorServerAcc");
XrdOucTrace TkTrace(&TkEroute);


#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>




#define IS_SLASH(s) (s == '/')

/**********************************************************************
 * signature with base64 encoding
 * 
 */

bool
XrdxCastor2ServerAcc::SignBase64(unsigned char *input, int inputlen, XrdOucString& sb64, int& sig64len, const char* keyclass)
{
  int err;
  unsigned int sig_len;
  unsigned int encodelength;
  int cnt=0;
  unsigned char sig_buf[16384];
  char signed_signature_buff[16384];

  XrdxCastor2Timing signtiming("SignBase64");

  TIMING(TkTrace,"START",&signtiming);

  EVP_MD_CTX     md_ctx;
  EVP_MD_CTX_init(&md_ctx);

  BIO *bmem, *b64;
  BUF_MEM *bptr;

  sb64 = "";
  /* use the envelope api to create an encode the hash value */
  //  EVP_SignInit_ex   (&md_ctx, EVP_sha1(),NULL);
  EVP_SignInit   (&md_ctx, EVP_sha1());
  EVP_SignUpdate (&md_ctx, input, inputlen);
  sig_len = sizeof(sig_buf);

  TIMING(TkTrace,"EVPINIT/UPDATE",&signtiming);

  encodeLock.Lock();
  EVP_PKEY* usekey = privatekey.Find(keyclass);
  if (!usekey) {
    // if not for this keyclass, look for default
    usekey = privatekey.Find("default");
  }

  if (!usekey) {
    TkTrace.Beg("SignBase64");
    cerr << "Unable to find privave key for key class " << keyclass << endl;
    TkTrace.End();
    return false;
  }

  err = EVP_SignFinal (&md_ctx, sig_buf, &sig_len, usekey);
  encodeLock.UnLock();
  TIMING(TkTrace,"EVPSIGNFINAL",&signtiming);

  if (!err) {
    TkTrace.Beg("SignBase64");
    cerr << "Unable to sign hash value. Check private key file. " << endl;
    TkTrace.End();
    return false;
  }

  EVP_MD_CTX_cleanup(&md_ctx);
  
  encodelength = sig_len;

  TIMING(TkTrace,"EVPCLEANUP",&signtiming);
  /* base64 encode */
  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, sig_buf,sig_len);
  BIO_flush(b64);
  BIO_get_mem_ptr(b64, &bptr);
  TIMING(TkTrace,"BASE64",&signtiming);
  char *buff = bptr->data;

  /* remove the backslash from the signature buffer */
  for (int i=0; i<=(bptr->length-1); i++) {
    if (buff[i] != '\n') {
      signed_signature_buff[cnt] = buff[i];
      cnt++;
    }
  }
  signed_signature_buff[cnt] = 0;
  sig64len = cnt;

  sb64 = signed_signature_buff;

  BIO_free(bmem);
  BIO_free(b64);
  TIMING(TkTrace,"BIOFREE",&signtiming);
  signtiming.Print(TkTrace);
  return true;
}


/**********************************************************************
 * verify the request signature
 */

bool
XrdxCastor2ServerAcc::VerifyUnbase64(const char* data, unsigned char *base64buffer, const char* path) {
  int err;
  int sig_len;
  int unbase64error=1;

  char sig_buf[16384];

  XrdxCastor2Timing verifytiming("VerifyUb64");

  TIMING(TkTrace,"START",&verifytiming);
  EVP_MD_CTX     md_ctx;

  /* base64 decode */
  while(1) {
    BIO *b64, *bmem;
    int cpcnt=0;
    char modinput[16384];
    int modlength;
    int i;
    int bread;
    int inputlen = strlen((const char*)base64buffer);
    unsigned char* input  = base64buffer;

    /* add the \n every 64 characters which have been removed to be compliant with the HTTP URL syntax */
    //    modinput = (char *) malloc(inputlen + (inputlen/64 +1)+1);
    //    if (!modinput) {
    //      break;
    //    }
    //    memset(modinput, 0, inputlen + (inputlen/64 + 1)+1);

    for (i=0;i<inputlen+1;i++) {
      /* fill a '\n' every 64 characters */
      if(i && (!(i%64))) {
	modinput[cpcnt]='\n';
	cpcnt++;
      }
      modinput[cpcnt] = input[i];
      cpcnt++;
    }
    modinput[cpcnt]=0;
    modlength=cpcnt-1;
    
    //    memset(sig_buf, 0, modlength);
    
    b64 = BIO_new(BIO_f_base64());
    if (!b64) 
      break;
    bmem = BIO_new_mem_buf(modinput, modlength);
    if (!bmem)
      break;
    bmem = BIO_push(b64, bmem);

    bread=BIO_read(bmem, sig_buf, modlength);

    BIO_free_all(bmem);
    
    //    free(modinput);

    sig_len = bread;

    if (sig_len)
      unbase64error = 0;
    break;
  }

  if (unbase64error) {
    TkEroute.Emsg("verifysignature",EACCES,"do base64 decoding for sfn", path);
    return false;
  }

  TIMING(TkTrace,"UNBASE64",&verifytiming);

  /* Verify the signature */
  //  printf("Verify %d %s %d %d\n",strlen((char*)data),data,publickey, sig_len);
  EVP_VerifyInit   (&md_ctx, EVP_sha1());
  EVP_VerifyUpdate (&md_ctx, data, strlen((char*)data));
  err = EVP_VerifyFinal (&md_ctx, ((unsigned char*)(sig_buf)), sig_len, publickey);

  EVP_MD_CTX_cleanup(&md_ctx);
  TIMING(TkTrace,"EVPVERIFY",&verifytiming);
  verifytiming.Print(TkTrace);
  if (err != 1) {
    return false;
  }
  return true;
}


char 
XrdxCastor2ServerAcc::x2c(const char *what)
{
  register char digit;
  
  digit = ((what[0] >= 'A') ? ((what[0] & 0xdf) - 'A') + 10
	   : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A') + 10
	    : (what[1] - '0'));
  return (digit);
}

void XrdxCastor2ServerAcc::unescape_url(char* url) 
{
  char *x, *y;
  
  /* Initial scan for first '%'. Don't bother writing values before
   * seeing a '%' */
  y = strchr(url, '%');
  if (y == NULL) {
    return;
  }
  for (x = y; *y; ++x, ++y) {
    if (*y != '%')
      *x = *y;
    else {
      if (!isxdigit(*(y + 1)) || !isxdigit(*(y + 2))) {
	*x = '%';
      }
      else {
	*x = x2c(y + 1);
	y += 2;
	//	if (IS_SLASH(*x) || *x == '\0')
      }
    }
  }
  *x = '\0';
  return;
}


XrdxCastor2ServerAcc::AuthzInfo*
XrdxCastor2ServerAcc::Decode(const char* opaque) 
{
  if (!opaque) return NULL;

  XrdOucString sop = opaque;
  // convert the '&' seperated tokens into '\n' seperated tokens
  sop.replace("&","\n");
  
  XrdOucTokenizer authztokens((char*)sop.c_str());

  XrdxCastor2ServerAcc::AuthzInfo* authz = new XrdxCastor2ServerAcc::AuthzInfo;

  const char* stoken;

  int ntoken=0;
  while( (stoken = authztokens.GetLine()) )  {
    XrdOucString token = stoken;

    // check the existance of the castor2fs token in the opaque info 
    if (token.beginswith("castor2fs.sfn=")) {
      authz->sfn   = strdup(token.c_str()+14);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.id=")) {
      authz->id    = strdup(token.c_str()+13);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.client_sec_uid=")) {
      authz->client_sec_uid = strdup(token.c_str()+25);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.client_sec_gid=")) {
      authz->client_sec_gid = strdup(token.c_str()+25);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.accessop=")) {
      authz->accessop = atoi(token.c_str()+19);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.exptime=")) {
      authz->exptime  = (time_t) strtol(token.c_str()+18,0,10);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.token=")) {
      authz->token = strdup(token.c_str()+16);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.signature=")) {
      authz->signature = strdup(token.c_str()+20);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.manager=")) {
      authz->manager = strdup(token.c_str()+18);
      ntoken++;continue;      
    }
    if (token.beginswith("castor2fs.pfn1=")) {
      authz->pfn1 = strdup(token.c_str()+15);
      ntoken++;continue;
    }
    if (token.beginswith("castor2fs.pfn2=")) {
      authz->pfn2 = strdup(token.c_str()+15);
      ntoken++;continue;
    }
  }


  // we expect 11 opaque castor2fs tokens to be present
  if (ntoken != 11) {
    //    printf("Wrong Number of tokens %d\n",ntoken);
    XrdOucString ntk="";
    ntk += ntoken;
    TkEroute.Emsg("Access",EACCES,ntk.c_str(), opaque);

    // possible memory leak - we should delete all members which have been filled here!
    authz->DeleteMembers(); delete authz;;
    return NULL;
  }
  
  return authz;
}

bool
XrdxCastor2ServerAcc::BuildToken(XrdxCastor2ServerAcc::AuthzInfo* authz, XrdOucString &token) 
{
  if (!authz)
    return false;
  
  token="";
  if (!authz->sfn) return false;
  if (!authz->id)  return false;
  if (!authz->client_sec_uid) return false;
  if (!authz->client_sec_gid) return false;
  if (!authz->manager) return false;

  token+= authz->sfn;
  token+= authz->pfn1;
  token+= authz->pfn2;
  token+= authz->id;
  token+= authz->client_sec_uid;
  token+= authz->client_sec_gid;
  token+= (int) authz->accessop;
  token+= (int) authz->exptime;
  token+= authz->manager;
  return true;
}

bool
XrdxCastor2ServerAcc::BuildOpaque(XrdxCastor2ServerAcc::AuthzInfo* authz, XrdOucString &opaque) 
{
  if (!authz)
    return false;
  opaque = "";

  
  if (!authz->sfn) return false;
  if (!authz->pfn1) return false;
  if (!authz->pfn2) return false;
  if (!authz->id)  return false;
  if (!authz->client_sec_uid) return false;
  if (!authz->client_sec_gid) return false;
  if (!authz->manager) return false;
  if (!authz->token) return false;
  
  opaque  = "castor2fs.sfn="; opaque += authz->sfn; opaque+="&";
  opaque += "castor2fs.pfn1="; opaque += authz->pfn1; opaque+="&";
  opaque += "castor2fs.pfn2="; opaque += authz->pfn2; opaque+="&";
  opaque += "castor2fs.id="; opaque += authz->id; opaque+="&";
  opaque += "castor2fs.client_sec_uid="; opaque += authz->client_sec_uid; opaque+="&";
  opaque += "castor2fs.client_sec_gid="; opaque += authz->client_sec_gid; opaque+="&";
  opaque += "castor2fs.accessop="; opaque += authz->accessop; opaque +="&";
  opaque += "castor2fs.exptime="; opaque += (int)authz->exptime; opaque += "&";
  opaque += "castor2fs.token="; opaque += authz->token; opaque += "&";
  opaque += "castor2fs.signature="; opaque += authz->signature; opaque+="&";
  opaque += "castor2fs.manager="; opaque += authz->manager; opaque+="&";
  return true;
}

XrdAccPrivs 
XrdxCastor2ServerAcc::Access(const XrdSecEntity    *Entity,
				       const char            *path,
				       const Access_Operation oper,
				       XrdOucEnv       *Env) 
{
  //  TkTrace.Beg("Access");
  decodeLock.Lock();    

  XrdOucString envstring="";
  int envlen=0;
  if (Env && (Env->Env(envlen))) {
    envstring.assign(Env->Env(envlen),0,envlen);
  }
  //  cerr << "path="<< path<< " operation=" <<(int)oper << " env="<< envstring.c_str();
  //  TkTrace.End();
  char* opaque=0;

  envlen=0;
  XrdxCastor2ServerAcc::AuthzInfo* authz;

  // do a bypass for the /proc file system, we check the client identity in the XrdxCastorOfsFile::open method
  XrdOucString spath = path;

  // we take care in XrdxCastorOfs::open that a user cannot give a fake opaque to get all permissions!
  if (Env->Get("castor2ofsproc") && (!strcmp(Env->Get("castor2ofsproc"),"true"))) {
    decodeLock.UnLock();    
    return XrdAccPriv_All;
  }

  // check for localhost host connection
  if (AllowLocalhost) {
    XrdOucString chost = Entity->host;
    if ((chost == "localhost" || (chost == "localhost.localdomain") || (chost == "127.0.0.1"))) {
      decodeLock.UnLock();    
      return XrdAccPriv_All;
    }
  }

  // check for xfer request
  // we must be very careful that all other requirements are checked within the OFS, to avoid security holes
  if (Env->Get("xferuuid") && (Env->Get("ofsgranted"))) {
    decodeLock.UnLock();    
    return XrdAccPriv_All;
  }

  time_t now = time(NULL);

  opaque = Env->Env(envlen);

  /* this is not nice, but ROOT puts a ? into the opaque string, if there is a user opaque info */

  if (!opaque) {
    decodeLock.UnLock();    
    return XrdAccPriv_None;
  }

  for (unsigned int i=0; i< strlen(opaque);i++) {
    if (opaque[i]=='?')
      opaque[i] = '&';
  }
  

  /* decode the authz information from the opaque info */
  if (RequireCapability && !(authz=Decode(opaque))) {
    TkEroute.Emsg("Access",EACCES,"decode access token for sfn=", path);
    decodeLock.UnLock();
    return XrdAccPriv_None;
  }
   
  if (RequireCapability) {
    /* verify that the token information is identical to the explicit tokens */
    XrdOucString reftoken;
    if (!BuildToken(authz, reftoken)) {
      TkEroute.Emsg("Access",EACCES,"build reference token for sfn=", path);
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
    
    if (reftoken != XrdOucString(authz->token)) {
      TkEroute.Emsg("Access",EACCES,"verify the provided token information - intrinsic tags differ from explicit tags for sfn=", path);
      printf("%s||||%s\n",reftoken.c_str(),authz->token);
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
    
    
    /* verify the signature of authz information */
    if ((!VerifyUnbase64( (const char*)authz->token, (unsigned char*)authz->signature, path))) {
      TkEroute.Emsg("Access",EACCES,"verify signature in request sfn=", path);
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
    
    /* check that path in token and request are identical */
    if (strcmp(path,authz->pfn1) && strcmp(path,authz->pfn2)) {
      // if it does not fit, check that it is a replica location
      XrdOucString spfn2 = authz->pfn2;
      if (!spfn2.find(authz->pfn1)) {
	TkEroute.Emsg("Access",EACCES,"give access - the signature was not provided for this path!");
	authz->DeleteMembers(); delete authz;;
	decodeLock.UnLock();
	return XrdAccPriv_None;
      }
    }
    
    /* check validity time in authz */
    if (authz->exptime < now) {
      TkEroute.Emsg("check_user_access",EACCES,"give access - the signature has expired already!");
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
    
    /* check that the connected client is identical to the client connected to the manager */
    if (0) {
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
    
    /* check that the client name is identical to the client name used at the manager */
    if (0) {
      authz->DeleteMembers(); delete authz;;
      decodeLock.UnLock();
      return XrdAccPriv_None;
    }
  
    authz->DeleteMembers(); delete authz;;
  }

  // if we have an open with a write we have to send a fsctl message to the manager !
  decodeLock.UnLock();
  return XrdAccPriv_All;
}

bool
XrdxCastor2ServerAcc::Configure(const char* ConfigFN) {
  char *var;
  const char *val;
  int  cfgFD, NoGo = 0;
  XrdOucStream Config(&TkEroute, getenv("XRDINSTANCE"));

  TkTrace.What=0x0;

  RequireCapability = false;
  StrictCapability = false;
  AllowLocalhost = true;
  AllowXfer = true;

  auth_certfile = NULL;
  if (!ConfigFN || !*ConfigFN) {
    TkEroute.Emsg("Config","Configuration file not specified.");
  } else {
    // Try to open the configuration file
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return TkEroute.Emsg("Config", errno, "open config file", ConfigFN);
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    //int nprivatekeys=0;
    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "xcastor2.",9)) {
	var+= 9;
	if (!strcmp("publickey",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument for xcastor2.publickey invalid.");NoGo=1;
	    auth_certfile = NULL;
			  
	  } else {
	    auth_certfile = strdup(val);
	  }
	}
	if (!strcmp("privatekey",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument for xcastor2.privatekey invalid.");NoGo=1;
	  } 
	  char* val2;
	  if (!(val2 = Config.GetWord())) {
	    TkEroute.Say("=====> xcastor2.privatekey file: " ,val , " tag: default");
	    auth_keyfile.Add("default",new XrdOucString(val));
	    auth_keylist = new XrdOucTList("default",0, auth_keylist);
	  } else {
	    XrdOucString printit="=====> xcastor2.privatekey file: ";
	    printit+=val; printit+=" tag: "; printit += val2;
	    TkEroute.Say(printit.c_str());
	    auth_keyfile.Add(val2, new XrdOucString(val));
	    auth_keylist = new XrdOucTList(val2, 0, auth_keylist);
	  }
	}

	if (!strcmp("serveracctrace",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument for xcastor2.serveracctrace invalid.");NoGo=1;
	  } else {
	    TkTrace.What = strtoll(var,(char**)NULL,10); 
	  }
	}
	
	if (!strcmp("capability",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument 2 for capbility missing. Can be <true>/1 or <false>/0"); NoGo=1;
	  } else {
	    if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1"))) || (!(strcmp(val,"lazy"))) ) {
	      RequireCapability = true;
	      if (!(strcmp(val,"lazy"))) {
		StrictCapability = false;
	      } else {
		StrictCapability = true;
	      }
	    } else {
	      if ( (!(strcmp(val,"false"))) || (!(strcmp(val,"0")))) {
		RequireCapability = false;
	      } else {
		TkEroute.Emsg("Config","argument 2 for capbility invalid. Can be <true>/1 or <false>/0"); NoGo=1;
	      }
	    }
	  }
	}

	if (!strcmp("allowxfer",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument 2 for allowxfer missing. Can be <true>/1 or <false>/0"); NoGo=1;
	  } else {
	    if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1"))) ) {
	      AllowXfer = true;
	    } else {
	      AllowXfer = false;
	    }
	  }
	}

	if (!strcmp("allowlocalhost",var)) {
	  if (!(val = Config.GetWord())) {
	    TkEroute.Emsg("Config","argument 2 for allowlocalhost missing. Can be <true>/1 or <false>/0"); NoGo=1;
	  } else {
	    if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1"))) ) {
	      AllowLocalhost = true;
	    } else {
	      AllowLocalhost = false;
	    }
	  }
	}
      }
    }

    if (RequireCapability) {
      if (StrictCapability) {
	TkEroute.Say("=====> xcastor2.capability     : required");
      } else{
	TkEroute.Say("=====> xcastor2.capability     : lazy/required - no authentication required");
      }
    } else {
      TkEroute.Say("=====> xcastor2.capability     : ignored/not needed");
    }

    if (AllowLocalhost) {
      TkEroute.Say("=====> xcastor2.allowlocalhost : true");
    } else {
      TkEroute.Say("=====> xcastor2.allowlocalhost : false");
    }
     
    if (AllowXfer) {
      TkEroute.Say("=====> xcastor2.allowxfer      : true");
    } else {
      TkEroute.Say("=====> xcastor2.allowxfer      : false");
    }

    const char* tp;
    int isMan = ((tp = getenv("XRDREDIRECT")) && !strcmp(tp, "R"));
    
    if (RequireCapability) {
      if (isMan) {
	if (auth_certfile)
	  free(auth_certfile);
	auth_certfile = NULL;
	if (!auth_keyfile.Num()) {
	  TkEroute.Emsg("Config","missing xcastor2.privatekey on manager host. (default)");NoGo=1;
	} 
      } else {
	if (!auth_certfile) {
	  TkEroute.Emsg("Config","missing xcastor2.publickey on server host.");NoGo=1;
	} else {
	  TkEroute.Say("=====> xcastor2.publickey      : ", auth_certfile,"");
	}
      }
    }
  }
  
    
  if (NoGo)
    return false;

  return true;
}

bool
XrdxCastor2ServerAcc::Init() {
  FILE* fpcert = 0;
  //FILE* fpkey  = 0;

  if (auth_certfile) {
    fpcert = fopen (auth_certfile,"r");
    if (fpcert == NULL) {
      TkEroute.Emsg("Init",errno,"open cert file ", auth_certfile);
      return false;
    }
    
    /* get the public key */
    x509public = PEM_read_X509(fpcert, NULL, NULL,NULL);
    fclose (fpcert);
    
    if (x509public == NULL) {
      TkEroute.Emsg("Init",EACCES,"access x509 in file ", auth_certfile);
      return false;
    } else {
      /* Get public key */
      publickey=X509_get_pubkey(x509public);
      if (publickey == NULL) {
	TkEroute.Emsg("Init",EACCES,"access public key in file ", auth_certfile);
	return false;
      }
    }
  }

  /* get the private key(s) */
  if (auth_keyfile.Num()) {
    XrdOucTList* NextKey = auth_keylist;
    while ( NextKey ) {
      XrdOucString* thiskey = auth_keyfile.Find(NextKey->text);
      FILE* fpkey = 0;
      if (!thiskey) {
	TkEroute.Emsg("Init",EINVAL,"find key file for class", thiskey->c_str());
	return false;
      }
      fpkey = fopen (thiskey->c_str(),"r");
      if (fpkey == NULL) {
	TkEroute.Emsg("Init",errno,"open key file ", thiskey->c_str());
	return false;
      }
      
      EVP_PKEY* lprivatekey;
      lprivatekey = PEM_read_PrivateKey(fpkey, NULL, NULL,NULL);
      fclose (fpkey);
      
      if (lprivatekey == NULL) {
	TkEroute.Emsg("Init",EACCES,"access private key in file ", thiskey->c_str());
	return false;
      }
      privatekey.Add(NextKey->text,lprivatekey);
      NextKey=NextKey->next;
    }
  }
  return true;
}

XrdxCastor2ServerAcc::~XrdxCastor2ServerAcc() {}
/* XrdAccAuthorizeObject() is called to obtain an instance of the auth object
   that will be used for all subsequent authorization decisions. If it returns
   a null pointer; initialization fails and the program exits. The args are:

   lp    -> XrdSysLogger to be tied to an XrdSysError object for messages
   cfn   -> The name of the configuration file
   parm  -> Parameters specified on the authlib directive. If none it is zero.
*/

extern "C" XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp,
                                              const char   *cfn,
                                              const char   *parm) 
{
  TkEroute.SetPrefix("XrdxCastor2ServerAcc::");
  TkEroute.logger(lp);
  TkEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
	       "xCastor2ServerAcc (xCastor File System) v 1.0");
  XrdAccAuthorize* acc = (XrdAccAuthorize*) new XrdxCastor2ServerAcc();
  if (!acc) {
     TkEroute.Say("------ xCastor2ServerAcc Allocation Failed!");
     return 0;
  }

  if (!((XrdxCastor2ServerAcc*)acc)->Configure(cfn) || (!((XrdxCastor2ServerAcc*)acc)->Init())) {
    TkEroute.Say("------ xCastor2ServerAcc Initialization Failed!");
    delete acc;
    return 0;
  } else {
    TkEroute.Say("------ xCastor2ServerAcc initialization completed");
    return acc;
  }

}

