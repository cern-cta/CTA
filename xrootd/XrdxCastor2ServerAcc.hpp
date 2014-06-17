/*******************************************************************************
 *                      XrdxCastor2ServerAcc.hh
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
 * @author Elvin Sindrilaru & Andreas Peters - CERN
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
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
/*-----------------------------------------------------------------------------*/

class XrdOucEnv;
class XrdSecEntity;

//------------------------------------------------------------------------------
//! Class XrdxCastor2ServerAcc
//------------------------------------------------------------------------------
class XrdxCastor2ServerAcc: public XrdAccAuthorize
{
  public:

    //--------------------------------------------------------------------------
    //! Struct AuthzInfo
    //! This struct can be used in two ways: either by taking ownership of the 
    //! object passed to it by using strdup as they are all chars and then in the 
    //! destructor one needs to free these objects by calling free(), or by not 
    //! taking ownership and then we don't need to do any memory management.
    //--------------------------------------------------------------------------
    struct AuthzInfo {

      bool takeOwnership;    ///< if true then we need to free memory in the destructor
      int accessop;          ///< the access operation allowed -> see XrdAcc/XrdAccAuthorize.hh
      time_t exptime;        ///< time when the authorization expires 
      char* sfn;             ///< sfn
      char* pfn1;            ///< physical mount filename 
      char* pfn2;            ///< physical replica filename, if exists otherwise '-'
      char* id;              ///< the client connection id 
      char* client_sec_uid;  ///< the sec identity eg. user name 
      char* client_sec_gid;  ///< the sec identity eg. group name 
      char* signature;       ///< signature
      char* manager;         ///< hostname of the managernode 

      
      //------------------------------------------------------------------------
      //! Constructor 
      //!
      //! @param ownObj if true then the class attribute will be dynamically 
      //!               allocated and the memeory must be freed in the destructor,
      //!               otherwise if false than we dont't need to do any 
      //!               memory management 
      //!
      //------------------------------------------------------------------------
      AuthzInfo( bool ownObj ) :
        takeOwnership( ownObj ), accessop( 0 ), exptime( 0 ), 
        sfn( NULL ), pfn1( NULL ), pfn2( NULL ), id( NULL ), client_sec_uid( NULL ),
        client_sec_gid( NULL ), signature( NULL ), manager( NULL )
      { }


      //------------------------------------------------------------------------
      //! Destructor 
      //------------------------------------------------------------------------
      ~AuthzInfo() {
        if ( takeOwnership ) {
          if ( sfn )            free( sfn );
          if ( pfn1 )           free( pfn1 );
          if ( pfn2 )           free( pfn2 );
          if ( id )             free( id );
          if ( client_sec_uid ) free( client_sec_uid );
          if ( client_sec_gid ) free( client_sec_gid );
          if ( signature )      free( signature );
          if ( manager )        free( manager );
        }
      }
    };


    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2ServerAcc();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2ServerAcc();


    //--------------------------------------------------------------------------
    //! Initalise the plugin
    //--------------------------------------------------------------------------
    bool Init();


    //--------------------------------------------------------------------------
    //! Configure the plugin
    //!
    //! @param conf_file path to configuration file 
    //!
    //! @return true if configuration successful, otherwise false 
    //! 
    //--------------------------------------------------------------------------
    bool Configure( const char* conf_file );


    //--------------------------------------------------------------------------
    //! Build opaque information based on the info in AuthzInfo structure
    //! 
    //! @param authz structure containing info about authorization
    //! @param stagehost the name of the stager concerned
    //! 
    //! @return the opaque information or empty string in case of error 
    //! 
    //--------------------------------------------------------------------------
    std::string BuildOpaque(XrdxCastor2ServerAcc::AuthzInfo &authz,
                            XrdOucString &stagehost,
                            bool issueCapability,
                            XrdOucString &map_path);


    //--------------------------------------------------------------------------
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
    //! @param keyclass key class to be used 
    //!
    //! @return true if signing was successful, otherwise false 
    //!
    //--------------------------------------------------------------------------
    bool  SignBase64( unsigned char* input,
                      int            inputlen,
                      XrdOucString&  sb64,
                      int&           sig64len,
                      const char*    keyclass = "default" );


    //--------------------------------------------------------------------------
    //!
    //! Indicates whether or not the user/host is permitted access to the
    //! path for the specified operation. The default implementation that is
    //! statically linked determines privileges by combining user, host, user group,
    //! and user/host netgroup privileges. If the operation is AOP_Any, then the
    //! actual privileges are returned and the caller may make subsequent tests using
    //! Test(). Otherwise, a non-zero value is returned if access is permitted or a
    //! zero value is returned is access is to be denied. Other iplementations may
    //! use other decision making schemes but the return values must mean the same.
    //!
    //! @param Entity authentication information
    //! @param path the logical path which is the target of oper
    //! @param oper the operation being attempted (see above)
    //! @param env environmental information at the time of the
    //! @param operation as supplied by the path CGI string. This is optional 
    //!                  and the pointer may be zero.
    //!
    //--------------------------------------------------------------------------
    virtual XrdAccPrivs Access( const XrdSecEntity*    Entity,
                                const char*            path,
                                const Access_Operation oper,
                                XrdOucEnv*             Env = 0 );


    //--------------------------------------------------------------------------
    //! Not used 
    //--------------------------------------------------------------------------
    virtual int Audit( const int              /*accok*/,
                       const XrdSecEntity*    /*Entity*/,
                       const char*            /*path*/,
                       const Access_Operation /*oper*/,
                       XrdOucEnv*             /*Env = 0*/ ) { return 0; }


    //--------------------------------------------------------------------------
    //! Check whether the specified operation is permitted. If permitted it
    //! returns a non-zero. Otherwise, zero is returned. Not used.
    //--------------------------------------------------------------------------
    virtual int Test( const XrdAccPrivs      /*priv*/,
                      const Access_Operation /*oper*/ ) { return 0; }

  private:

    //--------------------------------------------------------------------------
    //! The reverse of the SignBase64 method. For this to be successful the 
    //! hashed value of the data buffer must be the same as the value obtained 
    //! by decoding the signature using the public key.
    //! 
    //! @param data buffer containing the signed part of the original URL
    //! @param base64buffer signature buffer
    //! @param path file name path 
    //!
    //! @return true if data matches with the signature, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool VerifyUnbase64( const char*    data, 
                         unsigned char* base64buffer, 
                         const char*    path );


    char* auth_certfile;                    ///< file name of public key for signature verification
    XrdOucHash<XrdOucString> auth_keyfile;  ///< file name of private key for signature creation
    XrdOucTList* auth_keylist;

    bool RequireCapability; ///< client has to show up with a capability in the opaque info, if true
    bool StrictCapability;  ///< a client has to show up with a capability but we don't require 
                            ///< the authentication ID used in the capability e.g. we can run the 
                            ///< disk server without strong authentication!
    bool AllowLocalhost;    ///< a client connecting from localhost does not need authorization [default=yes]
    bool AllowXfer;         ///< a client coming from a transfer connection does not need authorization [default=yes]

    X509* x509public;
    EVP_PKEY*            publickey;
    XrdOucHash<EVP_PKEY> privatekey;
    XrdSysMutex          decodeLock;
    XrdSysMutex          encodeLock;
};

