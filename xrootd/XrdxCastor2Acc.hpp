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

#pragma once

/*-----------------------------------------------------------------------------*/
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
/*-----------------------------------------------------------------------------*/
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdAcc/XrdAccPrivs.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Logging.hpp"
/*-----------------------------------------------------------------------------*/

//! Forward declaration
class XrdOucEnv;
class XrdSecEntity;

//------------------------------------------------------------------------------
//! Class XrdxCastor2Acc
//------------------------------------------------------------------------------
class XrdxCastor2Acc: public XrdAccAuthorize, public LogId
{
  public:

  //----------------------------------------------------------------------------
  //! Struct AuthzInfo holding authorization information about a transfer
  //----------------------------------------------------------------------------
  struct AuthzInfo
  {
    AuthzInfo() : accessop(0), exptime(0) {};
    int accessop; ///< the access operation allowed -> see XrdAcc/XrdAccAuthorize.hh
    time_t exptime; ///< time when the authorization expires
    std::string sfn; ///< sfn
    std::string pfn1; ///< physical mount filename
    std::string pool; ///< ceph pool in case pfn1 denotes a ceph file (no leading '/')
    std::string pfn2; ///< stager job connection details
    std::string id; ///< the client connection id
    std::string client_sec_uid; ///< the sec identity eg. user name
    std::string client_sec_gid; ///< the sec identity eg. group name
    std::string signature; ///< signature of the 'token'
    std::string manager; ///< hostname of the managernode
  };


  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdxCastor2Acc();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdxCastor2Acc();


  //--------------------------------------------------------------------------
  //! Initialise the plugin
  //--------------------------------------------------------------------------
  bool Init();


  //----------------------------------------------------------------------------
  //! Configure the plugin
  //!
  //! @param conf_file path to configuration file
  //!
  //! @return true if configuration successful, otherwise false
  //----------------------------------------------------------------------------
  bool Configure(const char* conf_file);


  //----------------------------------------------------------------------------
  //! Build the authorization token from the information held in the AuthzInfo
  //! structure and sign all this with the private key of the server if the
  //! signature is required.
  //!
  //! @param authz AuthzInfo structure
  //! @param doSign if true sign, otherwise don't
  //!
  //! @return opaque information containing all the data in the AuthzInfo and
  //!         the signature to verify that the information was actually sent
  //!         by the XRootD headnode
  //----------------------------------------------------------------------------
  std::string GetOpaqueAcc(AuthzInfo& authz, bool doSign);


  //----------------------------------------------------------------------------
  //! Indicates whether or not the user/host is permitted access to the
  //! path for the specified operation. The default implementation that is
  //! statically linked determines privileges by combining user, host, user group,
  //! and user/host netgroup privileges. If the operation is AOP_Any, then the
  //! actual privileges are returned and the caller may make subsequent tests using
  //! Test(). Otherwise, a non-zero value is returned if access is permitted or a
  //! zero value is returned is access is to be denied. Other implementations may
  //! use other decision making schemes but the return values must mean the same.
  //!
  //! @param Entity authentication information
  //! @param path the logical path which is the target of oper
  //! @param oper the operation being attempted (see above)
  //! @param env environmental information at the time of the
  //! @param operation as supplied by the path CGI string. This is optional
  //!                  and the pointer may be zero.
  //----------------------------------------------------------------------------
  virtual XrdAccPrivs Access(const XrdSecEntity* Entity,
                             const char* path,
                             const Access_Operation oper,
                             XrdOucEnv* Env = 0);


  //----------------------------------------------------------------------------
  //! Not used
  //----------------------------------------------------------------------------
  virtual int Audit(const int /*accok*/,
                    const XrdSecEntity* /*Entity*/,
                    const char* /*path*/,
                    const Access_Operation /*oper*/,
                    XrdOucEnv* /*Env = 0*/)
  {
    return 0;
  }


  //----------------------------------------------------------------------------
  //! Check whether the specified operation is permitted. If permitted it
  //! returns a non-zero. Otherwise, zero is returned. Not used.
  //----------------------------------------------------------------------------
  virtual int Test(const XrdAccPrivs /*priv*/,
                   const Access_Operation /*oper*/)
  {
    return 0;
  }


 private:

  //----------------------------------------------------------------------------
  //! Build the autorization token used for signing. The token is made up of all
  //! the values of the parameters passed in the opaque information except of
  //! course the castor2fs.signature one.
  //!
  //! @param authz AuthzInfo used to build the token
  //!
  //! @return token string used later for signing
  //----------------------------------------------------------------------------
  std::string BuildToken(const AuthzInfo& authz);


  //----------------------------------------------------------------------------
  //! Signature with base64 encoding. First the implementation of the digest
  //! type is set ( sha1 ) then the input data is hased into the context. The
  //! next step is signing the hash value with the private key. The result is
  //! encoded using base64 encoding. Furthermore, as a last step all the '\n'
  //! are removed from the signature so that it is compliant with the HTTP URL
  //! syntax.
  //!
  //! @param input buffer to be signed
  //! @param inputlen length of the input buffer
  //! @param sb64 output buffer containgin the signature
  //! @param sb64len length of the signature
  //!
  //! @return true if signing was successful, otherwise false
  //----------------------------------------------------------------------------
  bool SignBase64(unsigned char* input,
                  int inputlen,
                  std::string& sb64,
                  int& sb64len);


  //----------------------------------------------------------------------------
  //! The reverse of the SignBase64 method. For this to be successful the
  //! hashed value of the data buffer must be the same as the value obtained
  //! by decrypting the signature using the public key
  //!
  //! @param data buffer containgin the full token information
  //! @param base64buffer signature buffer
  //! @param path file name path
  //!
  //! @return true if data matches with the signature, otherwise false
  //----------------------------------------------------------------------------
  bool VerifyUnbase64(const char* data,
                      unsigned char* base64buffer,
                      const char* path);


  //----------------------------------------------------------------------------
  //! Decode the opaque information
  //!
  //! @param opaque buffer containing opaque information
  //! @param authz structure populated with info from the opaque buffer
  //!
  //! @return true if decoding successful, otherwise false
  //----------------------------------------------------------------------------
  bool Decode(const char* opaque, AuthzInfo& authz);


  int mLogLevel; ///< acc plugin loglevel
  std::string mAuthCertfile; ///< file name of public key for signature verification
  std::string mAuthKeyfile; ///< file name of private key for signature creation
  bool mRequireCapability; ///< client has to show up with a capability in the
                           ///< opaque info, if true
  bool mAllowLocal; ///< a client connecting from localhost does not need
                    ///< authorization [default=yes]
  EVP_PKEY* mPublicKey; ///< public key used for decryption
  EVP_PKEY* mPrivateKey; ///< private key used for encryption
  XrdSysMutex mDecodeMutex; ///< mutex for decoding
  XrdSysMutex mEncodeMutex; ///< mutex for encoding
};
