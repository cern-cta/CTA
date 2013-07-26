/*******************************************************************************
 *                      XrdxCastorClient.hh
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
 * @author Elvin Sindrilaru, esindril@cern.ch - CERN 2013
 * 
 ******************************************************************************/

#ifndef __XCASTOR_CASTORCLIENT_HH__
#define __XCASTOR_CASTORCLIENT_HH__

/*----------------------------------------------------------------------------*/
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/times.h>
#include <sys/time.h>
#include <string>
#include <map>
/*----------------------------------------------------------------------------*/
#include "XrdxCastorNamespace.hpp"
#include "XrdxCastorLogging.hpp"
#include "XrdxCastor2FsConstants.hpp"
/*----------------------------------------------------------------------------*/
#include "castor/io/ServerSocket.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/client/IResponseHandler.hpp"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
/*----------------------------------------------------------------------------*/

XCASTORNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! Class XrdxCastorClient
//------------------------------------------------------------------------------
class XrdxCastorClient: public LogId
{
public :

  ///< Declare struct 
  struct ReqElement;

  //! Convenience typedef for map of async requests
  typedef std::map<std::string, struct ReqElement*> AsyncReqMap;
  typedef std::map<std::string, AsyncReqMap::iterator> AsyncUserMap;

  
  //----------------------------------------------------------------------------
  //! Obatin a new instance of the object while checking that the poller thread
  //! was also started successfully.
  //!
  //! @return new object instance 
  //----------------------------------------------------------------------------
  static XrdxCastorClient* Create();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdxCastorClient() throw ();


  //----------------------------------------------------------------------------
  //! Send asynchronous request 
  //!
  //! @param iterHint hint where the element was inserted
  //! @param userId user unique id for the request
  //! @param rhHost hostname of the stager to where the req is sent 
  //! @param rhPort port on the stager to where the req is sent 
  //! @param req request object 
  //! @param rh response handler object 
  //!
  //! @return SFS_OK if request sent successfully
  //!         SFS_STALL the client is to back-off, this happens when there are 
  //!                   already to many requests on-the-fly
  //!
  //----------------------------------------------------------------------------
  virtual int SendAsyncRequest(const std::string& userId,
                               const std::string& rhHost,
                               unsigned int rhPort, 
                               castor::stager::Request* req,
                               castor::client::IResponseHandler* rh,
                               std::vector<castor::rh::Response*>* respvec)
    throw(castor::exception::Exception);

  
  //----------------------------------------------------------------------------
  //! Get response from the stager about a request sent earlier
  //!
  //! @param userId user id
  //! @param foundReq boolean value to mark if we found the request
  //! @param isFirstTime boolean value signaling that we are trying to check for
  //!        a response immediately after submitting the request and not after
  //         doing a stall
  //!
  //! @return element containing the response, only if the response is 
  //!         available otherwise return 0
  //----------------------------------------------------------------------------
  struct ReqElement*
  GetResponse(const std::string& userId, bool& foundReq, bool isFirstTime);


  //----------------------------------------------------------------------------
  //! Check if the user has already submitted the current request. If so, this 
  //! means it comes back to collect the response after a stall. 
  //! 
  //! @param path file path for the request
  //! @param error error information
  //!
  //! @return true if request already submitted, otherwise false
  //!
  //----------------------------------------------------------------------------
  bool HasSubmittedReq(const char* path, XrdOucErrInfo& error);
  

  //----------------------------------------------------------------------------
  //! Method polling for responses from the stager and adding these responses 
  //! to the mMapRequests, setting the appropriate flag for the current request.
  //----------------------------------------------------------------------------
  virtual void PollResponses();


  //----------------------------------------------------------------------------
  //! Method used to strart the poller thread
  //!
  //! @param arg pointer to the XrdxCastorClient object
  //!
  //----------------------------------------------------------------------------
  static void* StartPollerThread(void* arg);


  //----------------------------------------------------------------------------
  //! Get the status of the poller thread 
  //!
  //! @return true when poller thread not started, otherwise false
  //----------------------------------------------------------------------------
  inline bool IsZombie()
  {
    return mIsZombie;
  }


  //----------------------------------------------------------------------------
  //! The ReqElement structure encapsule the request object sent to the stager 
  //! and also the response handler. These objects are stored in the mMapRequests
  //! and are accessed by both the asyn thread polling for responses and also
  //! by clients check-in for their responses.
  //----------------------------------------------------------------------------
  struct ReqElement
  {
    std::string mUserId;   ///< id of the user who submited the request 
    castor::stager::Request* mRequest;     ///< request object sent to stager
    castor::client::IResponseHandler* mRh; ///< response hadler 
    std::vector<castor::rh::Response*>* mRespVec; ///< vector of responses
    struct timeval mSendTime;  ///< time when request was sent to stager
    struct timeval mRecvTime;  ///< time when response was received from stager

    //--------------------------------------------------------------------------
    //! Constructor
    //! 
    //! @param req request submited to stager
    //! @param rh response handler object 
    //! 
    //--------------------------------------------------------------------------
    ReqElement(const std::string& userId,
               castor::stager::Request* req,
               castor::client::IResponseHandler* rh,
               std::vector<castor::rh::Response*>* respvec): 
      mUserId(userId),
      mRequest(req), 
      mRh(rh),
      mRespVec(respvec)
    {
      // The sendTime is the same as the creation of the object 
      if (gettimeofday(&mSendTime, NULL))
      {
        xcastor_static_err("could not get send time for request");
        timerclear(&mSendTime);
      }
      
      timerclear(&mRecvTime);
    }


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~ReqElement()
    {
      // Delete request 
      delete mRequest;
      
      // Delete the responses and the vector 
      delete mRh;
      castor::rh::Response* ptr = 0;
  
      for (unsigned int i = 0; i < mRespVec->size(); i++)
      {
        ptr = mRespVec->at(i);
        delete ptr;
      }
      
      mRespVec->clear();
      delete mRespVec;
    }

    //--------------------------------------------------------------------------
    //! Send the time when the response arrived 
    //--------------------------------------------------------------------------
    void SetResponseTime()
    {
      if (gettimeofday(&mRecvTime, NULL))
      {
        xcastor_static_err("could not get time for response");
        timerclear(&mRecvTime);
      }
    }


    //--------------------------------------------------------------------------
    //! Delete request if response received but user never turned up to collect 
    //! it. Here we use XCASTO2FS_RESP_TIMEOUT value to discard responses for
    //! which the client never showed up to collect them.
    //--------------------------------------------------------------------------
    bool Expired()
    {
      bool ret = false;
      struct timeval now;
      struct timeval res;

      if (gettimeofday(&now, NULL))
      {
        xcastor_static_err("could not get current time");
        return ret;
      }

      timersub(&now, &mRecvTime, &res);

      if (res.tv_sec > XCASTOR2FS_RESP_TIMEOUT)
      {
        xcastor_static_debug("response timeout, mark for deletion");
        ret = true;
      }

      return ret;
    }


    //--------------------------------------------------------------------------
    //! Decide if the response for the current request has arrived
    //--------------------------------------------------------------------------
    bool HasResponse()
    {
      return timercmp(&mRecvTime, &mSendTime, >);
    }

  };

private:

  //----------------------------------------------------------------------------
  //! Constructor
  //! 
  //! @param callbackPort callback port
  //!
  //----------------------------------------------------------------------------
  XrdxCastorClient(unsigned int callbackPort) 
    throw (castor::exception::Exception);


  //----------------------------------------------------------------------------
  //! Clean up old requests for which we received the answer from the stager
  //! but the client never showed up to collect them. This method is called with
  //! the mutex for the maps locked.
  //----------------------------------------------------------------------------
  virtual void DoCleanup();


  unsigned int mCallbackPort; ///< callback port used to listen for responses
  castor::io::ServerSocket* mCallbackSocket; ///< callback socket
  struct pollfd mFds[1024]; ///< set of file descriptors to wait on 
  nfds_t mNfds; ///< number of pollfs structs in the mFds array
  std::vector<castor::io::ServerSocket*> mConnected; ///< vector of accepted connections
  bool mDoStop;    ///< flag when to terminate the poller thread
  bool mIsZombie;   ///< status of the poller thread
  
  XrdSysThread mPollerThread;  ///< thread accepting responses from the stager
  XrdSysMutex mMutexMaps;      ///< mutex for the two maps
  XrdSysCondVar mCondResponse; ///< condition variable signalling the arrival of a response

  //! Mapping from  user ids (which are obtained by concatenating the type of
  //! request with the tident of the user and the path for the reuqest) to ReqElement 
  AsyncUserMap mMapUsers;
  
  //! Mapping from request ids got from the stager to ReqElement objects
  AsyncReqMap mMapRequests; 
  
};

XCASTORNAMESPACE_END

#endif // __XCASTOR_CASTORCLIENT_HH__
