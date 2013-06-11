/*******************************************************************************
 *                      XrdxCastorLogging.cc
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

/*----------------------------------------------------------------------------*/
#include "XrdxCastorLogging.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Global static variables
//------------------------------------------------------------------------------
int Logging::gLogMask=0;
int Logging::gPriorityLevel=0;
int Logging::gShortFormat=0;

Logging::LogArray         Logging::gLogMemory;
Logging::LogCircularIndex Logging::gLogCircularIndex;
unsigned long             Logging::gCircularIndexSize;

XrdSysMutex Logging::gMutex;
XrdOucString Logging::gUnit="none";
XrdOucString Logging::gFilter="";

VirtualIdentity Logging::gZeroVid;


/******************************************************************************/
/*                                L o g I d                                   */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LogId::LogId()
{
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid,logId);
  sprintf(cident,"<service>");
  vid = (int)getuid();
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LogId::~LogId() {}


//------------------------------------------------------------------------------
// For calls which are not client initiated this function set's a unique dummy log id
//------------------------------------------------------------------------------
void
LogId::SetSingleShotLogId( const char* td )
{
  snprintf(logId,sizeof(logId)-1,"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
  snprintf(cident, sizeof(cident)-1,"%s",td);
}


//------------------------------------------------------------------------------
// Set's the logid and trace identifier
//------------------------------------------------------------------------------
void
LogId::SetLogId(const char* newlogid, const char* td )
{
  if (newlogid != logId)
    snprintf(logId,sizeof(logId)-1,"%s",newlogid);
  snprintf(cident,sizeof(cident)-1,"%s", td);
}


/******************************************************************************/
/*                              L o g g i n g                                 */
/******************************************************************************/

//------------------------------------------------------------------------------
// Initialize the circular index and logging object
//------------------------------------------------------------------------------
void
Logging::Init()
{
  // Initialize the log array and sets the log circular size
  gLogCircularIndex.resize(LOG_DEBUG+1);
  gLogMemory.resize(LOG_DEBUG+1);
  gCircularIndexSize = XCASTORCOMMONLOGGING_CIRCULARINDEXSIZE;
  for (int i = 0; i<= LOG_DEBUG; i++ ) {
    gLogCircularIndex[i] = 0;
    gLogMemory[i].resize(gCircularIndexSize);
  }
  gZeroVid = "-";
}


//------------------------------------------------------------------------------
// Should log function
//------------------------------------------------------------------------------
bool
Logging::shouldlog(const char* func, int priority)
{
  // Short cut if log messages are masked
  if (!((LOG_MASK(priority) & gLogMask)))
    return false;

  // Apply filter to avoid message flooding for debug messages
  if (priority >= LOG_INFO) {
    if ( (gFilter.find(func))!=STR_NPOS) {
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// Logging function
//------------------------------------------------------------------------------
const char*
Logging::log(const char*                     func,
             const char*                     file,
             int                             line,
             const char*                     logid,
             const VirtualIdentity&          vid,
             const char*                     cident,
             int                             priority,
             const char*                     msg, ...)
{
  static int logmsgbuffersize=1024*1024;
  static char* buffer=0;

  if (!shouldlog(func, priority)) {
    return "";
  }

  if (!buffer) {
    // 1MB print buffer
    buffer = (char*) malloc(logmsgbuffersize);
  }

  XrdOucString File = file;

  // We truncate the file name and show only the end
  if (File.length() > 16) {
    int up = File.length() - 13;
    File.erase(3, up);
    File.insert("...",3);
  }

  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm *tm;
  gMutex.Lock();

  va_list args;
  va_start (args, msg);

  time (&current_time);
  gettimeofday(&tv, &tz);

  static char linen[16];
  sprintf(linen,"%d",line);

  static char fcident[1024];
  XrdOucString truncname = vid;

  // We show only the last 16 bytes of the name
  if (truncname.length() > 16) {
    truncname.insert("..",0);
    truncname.erase(0,truncname.length()-16);
  }

  if (gShortFormat) {
    tm = localtime (&current_time);
    sprintf (buffer, "%02d%02d%02d %02d:%02d:%02d time=%lu.%06lu func=%-12s level=%s tid=%lu source=%s:%-5s ",
             tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time,
             (unsigned long)tv.tv_usec, func, GetPriorityString(priority), (unsigned long)XrdSysThread::ID(),
             File.c_str(), linen);
  } else {
    sprintf( fcident,"tident=%s vid=%s ", cident, vid.c_str() );
    tm = localtime (&current_time);
    sprintf (buffer, "%02d%02d%02d %02d:%02d:%02d time=%lu.%06lu func=%-24s level=%s logid=%s unit=%s tid=%lu source=%s:%-5s %s ",
             tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time,
             (unsigned long)tv.tv_usec, func, GetPriorityString(priority),logid, gUnit.c_str(),
             (unsigned long)XrdSysThread::ID(), File.c_str(), linen, fcident);
  }

  char*  ptr = buffer + strlen(buffer);

  // Limit the length of the output to buffer-1 length
  vsnprintf(ptr, logmsgbuffersize - (ptr-buffer-1), msg, args);

  fprintf(stderr,"%s",buffer);
  fprintf(stderr,"\n");
  fflush(stderr);
  va_end(args);

  const char* rptr;
  // Store into global log memory
  gLogMemory[priority][(gLogCircularIndex[priority])%gCircularIndexSize] = buffer;
  rptr = gLogMemory[priority][(gLogCircularIndex[priority])%gCircularIndexSize].c_str();
  gLogCircularIndex[priority]++;
  gMutex.UnLock();
  return rptr;
}


//------------------------------------------------------------------------------
// Return priority int from string
//- ----------------------------------------------------------------------------
int
Logging::GetPriorityByString(const char* pri)
{
  if (!strncmp(pri,"info", 4))    return LOG_INFO;
  if (!strncmp(pri,"debug", 5))   return LOG_DEBUG;
  if (!strncmp(pri,"err", 3))     return LOG_ERR;
  if (!strncmp(pri,"emerg", 5))   return LOG_EMERG;
  if (!strncmp(pri,"alert", 5))   return LOG_ALERT;
  if (!strncmp(pri,"crit", 4))    return LOG_CRIT;
  if (!strncmp(pri,"warning", 7)) return LOG_WARNING;
  if (!strncmp(pri,"notice", 6))  return LOG_NOTICE;
  return -1;
}


//------------------------------------------------------------------------------
// Return priority as string
//------------------------------------------------------------------------------
const char*
Logging::GetPriorityString( int pri )
{
  if (pri==(LOG_INFO))    return "INFO ";
  if (pri==(LOG_DEBUG))   return "DEBUG";
  if (pri==(LOG_ERR))     return "ERROR";
  if (pri==(LOG_EMERG))   return "EMERG";
  if (pri==(LOG_ALERT))   return "ALERT";
  if (pri==(LOG_CRIT))    return "CRIT ";
  if (pri==(LOG_WARNING)) return "WARN ";
  if (pri==(LOG_NOTICE))  return "NOTE ";
  return "NONE ";
}


//------------------------------------------------------------------------------
// Set the log priority (like syslog)
//------------------------------------------------------------------------------
void
Logging::SetLogPriority(int pri)
{
  gPriorityLevel = pri;
  gLogMask = LOG_UPTO( gPriorityLevel);
}


//------------------------------------------------------------------------------
// Set the log unit name
//------------------------------------------------------------------------------
void
Logging::SetUnit(const char* unit)
{
  gUnit = unit;
}


//------------------------------------------------------------------------------
// Set the log filter
//------------------------------------------------------------------------------
void
Logging::SetFilter(const char* filter)
{
  gFilter = filter;
}









