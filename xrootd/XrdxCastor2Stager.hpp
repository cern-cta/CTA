/*******************************************************************************
 *                      XrdxCastor2Stager.hh
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

#ifndef __XCASTOR_STAGER_HH__
#define __XCASTOR_STAGER_HH__


/*-----------------------------------------------------------------------------*/
#include "XrdxCastorLogging.hpp"
#include "XrdxCastorClient.hpp"
/*-----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdOuc/XrdOucString.hh>
/*-----------------------------------------------------------------------------*/


//! Forward declarations
namespace castor
{
  namespace stager
  {
    class FileRequest;
  }

  namespace client
  {
    class IResponseHandler;
  }

  namespace rh
  {
    class Response;
  }
}


//------------------------------------------------------------------------------
//! Class XrdxCastor2Stager
//------------------------------------------------------------------------------
class XrdxCastor2Stager : public LogId
{
  public:

    ///! Delay store for each of the users
    static XrdOucHash<XrdOucString>* msDelayStore;
    static XrdSysRWLock msLockDelay; ///< RW lock for the delay map

    
    //--------------------------------------------------------------------------
    //! Request information structure
    //--------------------------------------------------------------------------
    struct ReqInfo
    {
       uid_t mUid;
       gid_t mGid;
       const char* mPath;
       const char* mStageHost;
       const char* mServiceClass;

       //-----------------------------------------------------------------------
       //! Constructor
       //-----------------------------------------------------------------------
       ReqInfo(uid_t uid, gid_t gid, const char* path, 
              const char* stagehost, const char* serviceclass):
         mUid(uid),
         mGid(gid),
         mPath(path),
         mStageHost(stagehost),
         mServiceClass(serviceclass)
       {
         // empty
       }
    };
  


    //--------------------------------------------------------------------------
    //! Response information structure
    //--------------------------------------------------------------------------
    struct RespInfo
    {
       XrdOucString mRedirectionHost;
       XrdOucString mRedirectionPfn1;
       XrdOucString mRedirectionPfn2;
       XrdOucString mStageStatus;
       
       //-----------------------------------------------------------------------
       //! Constructor
       //-----------------------------------------------------------------------
       RespInfo():
         mRedirectionHost(""),
         mRedirectionPfn1(""),
         mRedirectionPfn2(""),
         mStageStatus("")
       {
         //empty
       }
    };


    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Stager(): LogId() {};


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~XrdxCastor2Stager() {};


    //--------------------------------------------------------------------------
    //! Get a delay value for the corresponding tag 
    //!
    //! @param tag is made by concatenating the tident with the path of the req
    //!
    //! @return delay in seconds
    //--------------------------------------------------------------------------
    static int GetDelayValue(const char* tag);


    //------------------------------------------------------------------------------
    //! Drop delay tag from mapping
    //!
    //! @param tag tag to be dropped from mapping
    //!
    //------------------------------------------------------------------------------
    static void DropDelayTag(const char* tag);


    //--------------------------------------------------------------------------
    //! Delete the request and response objects from the maps
    //!
    //! @param req request object 
    //! @param resp response object 
    //! @param respvect response vector
    //!
    //--------------------------------------------------------------------------
    static void DeleteReqResp(castor::stager::FileRequest* req,
                              castor::client::IResponseHandler* resp,
                              std::vector<castor::rh::Response*>* respvec);


  
    //--------------------------------------------------------------------------
    //! Process response received form the stager
    //! 
    //! @param error error object
    //! @param respElem ReqElemement structure defined in XrdxCastorClient.hh
    //! @param opType opertation type
    //! @param reqInfo request informatio structure
    //! @param respInfo response info structure to be filled in
    //!
    //! @return 
    //--------------------------------------------------------------------------
    static int ProcessResponse(XrdOucErrInfo& error,
                               struct xcastor::XrdxCastorClient::ReqElement*& respElem,
                               const std::string& opType,
                               struct ReqInfo* reqInfo,
                               struct RespInfo& respInfo);
  

    //--------------------------------------------------------------------------
    //! Prepare2Get
    //--------------------------------------------------------------------------
    static bool Prepare2Get( XrdOucErrInfo& error,
                             uid_t uid, gid_t gid,
                             const char* path,
                             const char* stagehost,
                             const char* serviceclass,
                             struct RespInfo& respInfo);

    //--------------------------------------------------------------------------
    //! Send an async request to the stager. This request can be a GET or a PUT
    //! or an UPDATE.
    //!
    //! @param error error object 
    //! @param opType type of operation: get, put or update
    //! @param reqInfo request information stucture
    //! @param respInfo response information structure
    //! 
    //! @return SFS_OK answer received and successfully parsed
    //!         SFS_ERROR there was an error
    //!         SFS_STALL response not available yet, stall the client 
    //!         
    //--------------------------------------------------------------------------
    static int DoAsyncReq(XrdOucErrInfo& error,
                          const std::string& opType,
                          struct ReqInfo* reqInfo,
                          struct RespInfo& respInfo);


    //--------------------------------------------------------------------------
    //! Rm
    //--------------------------------------------------------------------------
    static bool Rm( XrdOucErrInfo& error,
                    uid_t uid, gid_t gid,
                    const char* path,
                    const char* stagehost,
                    const char* serviceclass );
  

    //--------------------------------------------------------------------------
    //! UpdateDone
    //--------------------------------------------------------------------------
    static bool UpdateDone( XrdOucErrInfo& error,
                            const char* path,
                            const char* reqid,
                            const char* fileid,
                            const char* nameserver,
                            const char* stagehost,
                            const char* serviceclass );


    //--------------------------------------------------------------------------
    //! StagerQuery
    //--------------------------------------------------------------------------
    static bool StagerQuery( XrdOucErrInfo& error,
                             uid_t uid, gid_t gid,
                             const char* path,
                             const char* stagehost,
                             const char* serviceclass,
                             XrdOucString& stagestatus );


    //--------------------------------------------------------------------------
    //! FirstWrite
    //--------------------------------------------------------------------------
    static bool FirstWrite( XrdOucErrInfo& error,
                            const char* path,
                            const char* reqid,
                            const char* fileid,
                            const char* nameserver,
                            const char* stagehost,
                            const char* serviceclass );
};

#endif // __XCASTOR_STAGER_HH__
