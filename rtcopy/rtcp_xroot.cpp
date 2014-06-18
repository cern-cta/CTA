/*
 * $Id: rtcpd.c,v 1.8 2009/08/18 09:43:01 waldron Exp $
 *
 * Copyright (C) 1999-2012 by CERN IT
 * All rights reserved
 */

/*
** An xroot wrapper for file commands used by rtcpd.
** We need it to be able to use any compilation flags specific only for xroot
*/ 

extern "C" {
#include <rtcp_xroot.h>
}

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#define _FILE_OFFSET_BITS 64

#include <sstream>
#include <string>
#include <algorithm>

#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>

#include <log.h>
#include <rtcp.h>
#include <errno.h>
#include <common.h>

#include <pthread.h>

#include "castor/exception/Exception.hpp"
#include <XrdPosix/XrdPosixExtern.hh>

// this lock allows to serialize accesses to EVP_SignFinal, as it's not thread safe
static pthread_mutex_t s_lockEVP = PTHREAD_MUTEX_INITIALIZER;

static int signXrootUrl(std::ostringstream &url, const std::string& input) {
  // get the private key to be used to sign our hash
  const char* pkeyFileName = getconfent("XROOT","PrivateKey",0);
  if (NULL == pkeyFileName) {
    pkeyFileName = "/opt/xrootd/keys/key.pem";
  }
  FILE* fpkey = fopen(pkeyFileName, "r");
  if (NULL == fpkey) {
    rtcp_log(LOG_ERR, "signXrootUrl: could not open private key file %s. Error was %s\n",
             pkeyFileName, sstrerror(errno));
    return -1;
  }
  EVP_PKEY* key = PEM_read_PrivateKey(fpkey, NULL, NULL, NULL);
  if (NULL == key) {
    rtcp_log(LOG_ERR, "signXrootUrl: could not open read private key from file %s\n",
             pkeyFileName);
    return -1;
  }
  fclose(fpkey);

  // Create a context and hash the data
  EVP_MD_CTX md_ctx;
  EVP_MD_CTX_init(&md_ctx);
  EVP_SignInit(&md_ctx, EVP_sha1());
  if (!EVP_SignUpdate(&md_ctx, input.c_str(), input.size())) {
    rtcp_log(LOG_ERR, "signXrootUrl: EVP_SignUpdate failed with code %u\n", ERR_get_error());
    EVP_MD_CTX_cleanup(&md_ctx);
    return -1;
  }

  // Sign the hash. Note that we need to serialize this code
  unsigned char sig_buf[16384];
  unsigned int sig_len = sizeof(sig_buf);
  int rc = pthread_mutex_lock(&s_lockEVP);
  if (rc) {
    rtcp_log(LOG_ERR, "signXrootUrl: Cannot acquire lock to secure call to EVP_SignFinal. Error code : %u\n", rc);
    EVP_MD_CTX_cleanup(&md_ctx);
    return -1;
  }
  int err = EVP_SignFinal(&md_ctx, sig_buf, &sig_len, key);
  pthread_mutex_unlock(&s_lockEVP);
  if (!err) {
    rtcp_log(LOG_ERR, "signXrootUrl: Unable to sign hash value. Error code : %u\n", err);
    EVP_MD_CTX_cleanup(&md_ctx);
    return -1;
  }
  // cleanup context
  EVP_MD_CTX_cleanup(&md_ctx);

  // base64 encode 
  BIO *b64 = BIO_new(BIO_f_base64());
  BIO *bmem = BIO_new(BIO_s_mem());
  if (NULL == b64 || NULL == bmem) {
    rtcp_log(LOG_ERR, "signXrootUrl: Unable to create BIO for b64 encoding\n");
    return -1;
  }
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, sig_buf, sig_len);
  (void)BIO_flush(b64);
  BUF_MEM* bptr;
  BIO_get_mem_ptr(b64, &bptr);

  // Remove the backslash from the signature buffer and cleanup memory
  std::string signature = bptr->data;
  signature.erase(std::remove(signature.begin(), signature.end(), '\n'), signature.end());
  BIO_free(bmem);
  BIO_free(b64);

  // complete the URL with the signature
  url << "&castor2fs.signature=" << signature;
  return 0;
}

/**
 * Convert the server:/path string to the CASTOR xroot
 * root://server:port//dummy?castor2fs.pfn1=path
 * @param rtcpPath the path to be converted.
 * @param xrootFilePath the variable to return xroot path.
 * @return 0 if convertion succeed and -1 in error case.
 */
static std::string rtcpToCastorXroot(const char *const rtcpPath) {
    std::string path(rtcpPath);
    size_t slashPos = path.find('/');
    if (std::string::npos == slashPos) {
      rtcp_log(LOG_ERR, "rtcpToCastorXroot: invalid path %s\n", rtcpPath);
      throw castor::exception::Exception();
    }
    // start of URL
    std::ostringstream ss;
    ss << "root://" << path.substr(0, slashPos) << "1095//dummy?";
    // "opaque" info
    std::ostringstream opaque;
    opaque << "castor2fs.pfn1=" << path.substr(slashPos);
    time_t exptime = time(NULL) + 3600;
    opaque << "&castor2fs.exptime=" << exptime;
    ss << opaque.str();
    // signature
    if (signXrootUrl(ss, opaque.str())) {
      throw castor::exception::Exception();
    }
    return ss.str();
}

extern "C" {
  
int        rtcp_xroot_stat(const char *path, struct stat *buf) {
  return XrdPosix_Stat(path,buf);
}

int        rtcp_xroot_open(const char *path, int oflag, int mode) {
  try {
    std::string xrootURL = rtcpToCastorXroot(path);
    return XrdPosix_Open(xrootURL.c_str(), oflag, mode);
  } catch (castor::exception::Exception e) {
    errno = EFAULT; /* sets  "Bad address" errno */
    return -1;
  }
}

long long  rtcp_xroot_lseek(int fildes, long long offset, int whence) {
  return XrdPosix_Lseek(fildes, offset, whence);
}

int        rtcp_xroot_close(int fildes) {
  return XrdPosix_Close(fildes);
}

long long  rtcp_xroot_write(int fildes, const void *buf, 
                            unsigned long long nbyte) {
  return XrdPosix_Write(fildes, buf, nbyte);
}

long long  rtcp_xroot_read(int fildes, void *buf, unsigned long long nbyte) {
  return XrdPosix_Read(fildes, buf, nbyte);
}

} // extern "C"
