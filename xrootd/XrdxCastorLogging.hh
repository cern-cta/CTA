/*******************************************************************************
 *                      XrdxCastorLogging.hh
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

#ifndef __XCASTOR_LOGGING_HH__
#define __XCASTOR_LOGGING_HH__

/*----------------------------------------------------------------------------*/
#include <string.h>
#include <sys/syslog.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string>
#include <vector>
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysLogger.hh"
/*----------------------------------------------------------------------------*/

typedef XrdOucString VirtualIdentity;

//------------------------------------------------------------------------------
//! Log Macros usable in objects inheriting from the logId Class
//------------------------------------------------------------------------------
#define xcastor_log(__XCASTIRCOMMON_LOG_PRIORITY__ , ...) Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, this->uid, this->gid,this->ruid, this->rgid, this->cident,  LOG_MASK(__XCASTORCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define xcastor_debug(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_DEBUG)  , __VA_ARGS__)
#define xcastor_info(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_INFO)   , __VA_ARGS__)
#define xcastor_notice(...)  Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_NOTICE) , __VA_ARGS__)
#define xcastor_warning(...) Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_WARNING), __VA_ARGS__)
#define xcastor_err(...)     Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_ERR)    , __VA_ARGS__)
#define xcastor_crit(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_CRIT)   , __VA_ARGS__)
#define xcastor_alert(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_ALERT)  , __VA_ARGS__)
#define xcastor_emerg(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, this->logId, vid, this->cident, (LOG_EMERG)  , __VA_ARGS__)

//------------------------------------------------------------------------------
//! Log Macros usable in singleton objects used by individual threads
//! You should define locally LodId ThreadLogId in the thread function
//------------------------------------------------------------------------------
#define xcastor_thread_debug(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_DEBUG)  , __VA_ARGS__)
#define xcastor_thread_info(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_INFO)   , __VA_ARGS__)
#define xcastor_thread_notice(...)  Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_NOTICE) , __VA_ARGS__)
#define xcastor_thread_warning(...) Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_WARNING), __VA_ARGS__)
#define xcastor_thread_err(...)     Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_ERR)    , __VA_ARGS__)
#define xcastor_thread_crit(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_CRIT)   , __VA_ARGS__)
#define xcastor_thread_alert(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_ALERT)  , __VA_ARGS__)
#define xcastor_thread_emerg(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, ThreadLogId.logId, vid, ThreadLogId.cident, (LOG_EMERG)  , __VA_ARGS__)

//------------------------------------------------------------------------------
//! Log Macros usable from static member functions without LogId object
//------------------------------------------------------------------------------
#define xcastor_static_log(__XCASTORCOMMON_LOG_PRIORITY__ , ...) Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", 0,0,0,0,"",  (__XCASTORCOMMON_LOG_PRIORITY__) , __VA_ARGS__
#define xcastor_static_debug(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_DEBUG)  , __VA_ARGS__)
#define xcastor_static_info(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_INFO)   , __VA_ARGS__)
#define xcastor_static_notice(...)  Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_NOTICE) , __VA_ARGS__)
#define xcastor_static_warning(...) Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_WARNING), __VA_ARGS__)
#define xcastor_static_err(...)     Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_ERR)    , __VA_ARGS__)
#define xcastor_static_crit(...)    Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_CRIT)   , __VA_ARGS__)
#define xcastor_static_alert(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_ALERT)  , __VA_ARGS__)
#define xcastor_static_emerg(...)   Logging::log(__FUNCTION__,__FILE__, __LINE__, "static", Logging::gZeroVid,"", (LOG_EMERG)  , __VA_ARGS__)

//------------------------------------------------------------------------------
//! Log Macros to check if a function would log in a certain log level
//------------------------------------------------------------------------------
#define XCASTOR_LOGS_DEBUG   Logging::shouldlog(__FUNCTION__,(LOG_DEBUG)  )
#define XCASTOR_LOGS_INFO    Logging::shouldlog(__FUNCTION__,(LOG_INFO)   )
#define XCASTOR_LOGS_NOTICE  Logging::shouldlog(__FUNCTION__,(LOG_NOTICE) )
#define XCASTOR_LOGS_WARNING Logging::shouldlog(__FUNCTION__,(LOG_WARNING))
#define XCASTOR_LOGS_ERR     Logging::shouldlog(__FUNCTION__,(LOG_ERR)    )
#define XCASTOR_LOGS_CRIT    Logging::shouldlog(__FUNCTION__,(LOG_CRIT)   )
#define XCASTOR_LOGS_ALERT   Logging::shouldlog(__FUNCTION__,(LOG_ALERT)  )
#define XCASTOR_LOGS_EMERG   Logging::shouldlog(__FUNCTION__,(LOG_EMERG)  )


#define XCASTORCOMMONLOGGING_CIRCULARINDEXSIZE 10000

//------------------------------------------------------------------------------
//! Class implementing XCASTOR logging
//------------------------------------------------------------------------------
class LogId {
public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    LogId();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~LogId();


    //--------------------------------------------------------------------------
    //! For calls which are not client initiated this function set's a unique 
    //! dummy log id
    //--------------------------------------------------------------------------
    void SetSingleShotLogId(const char* td="<single-exec>");


    //--------------------------------------------------------------------------
    //! Set's the logid and trace identifier
    //--------------------------------------------------------------------------
    void SetLogId(const char* newlogid, const char* td= "<service>");


    char logId[40];       ///< the log Id for message printout
    char cident[256];     ///< the client identifier
    VirtualIdentity vid;  ///< client identity
  
};


//------------------------------------------------------------------------------
//! Class wrapping global singleton objects for logging
//------------------------------------------------------------------------------
class Logging {
public:
  
    typedef std::vector< std::vector <XrdOucString> > LogArray; ///< typdef for log message array
    typedef std::vector< unsigned long > LogCircularIndex; ///< typedef for circular index pointing
                                                           ///< to the next message position int the log array

    static LogCircularIndex gLogCircularIndex;  ///< global circular index
    static LogArray         gLogMemory;         ///< global logging memory
    static unsigned long    gCircularIndexSize; ///< global circular index size
    static VirtualIdentity  gZeroVid;           ///< root vid
    static int              gLogMask;           ///< log mask
    static int              gPriorityLevel;     ///< log priority
    static XrdSysMutex      gMutex;             ///< global mutex
    static XrdOucString     gUnit;              ///< global unit name
    static XrdOucString     gFilter;            ///< global log filter to apply
    static int              gShortFormat;       ///< indicating if the log-output is in short format


    //--------------------------------------------------------------------------
    //! Initialize Logger
    //--------------------------------------------------------------------------
    static void Init();


    //--------------------------------------------------------------------------
    //! Set the log priority (like syslog)
    //--------------------------------------------------------------------------
    static void SetLogPriority(int pri);


    //--------------------------------------------------------------------------
    //! Set the log unit name
    //--------------------------------------------------------------------------
    static void SetUnit(const char* unit);


    //--------------------------------------------------------------------------
    //! Set the log filter
    //--------------------------------------------------------------------------
    static void SetFilter(const char* filter);


    //--------------------------------------------------------------------------
    //! Return priority as string
    //--------------------------------------------------------------------------
    static const char* GetPriorityString(int pri);


    //--------------------------------------------------------------------------
    //! Return priority int from string
    //--------------------------------------------------------------------------
    static int GetPriorityByString(const char* pri) ;


    //--------------------------------------------------------------------------
    //! Check if we should log in the defined level/filter
    //!
    //! @param func name of the calling function
    //! @param priority priority level of the message
    //!
    //--------------------------------------------------------------------------
    static bool shouldlog(const char* func, int priority);


    //--------------------------------------------------------------------------
    //! Log a message into the global buffer
    //!
    //! @param func name of the calling function
    //! @param file name of the source file calling
    //! @param line line in the source file
    //! @param logid log message identifier
    //! @param vid virtual id of the caller
    //! @param cident client identifier
    //! @param priority priority level of the message
    //! @param msg the actual log message
    //!
    //! @return pointer to the log message
    //!
    //--------------------------------------------------------------------------
    static const char* log(const char*                     func,
                           const char*                     file,
                           int                             line,
                           const char*                     logid,
                           const VirtualIdentity&          vid ,
                           const char*                     cident,
                           int                             priority,
                           const char*                     msg, ...);
};

#endif // __XCASTOR_LOGGING_HH__
