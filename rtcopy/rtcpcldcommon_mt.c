/******************************************************************************
 *                      rtcpcldcommon_mt.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @(#)$RCSfile: rtcpcldcommon_mt.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/12 08:37:03 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

/**
 * \file rtcpcldcommon_mt.c - common multithreaded methods
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldcommon_mt.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/12 08:37:03 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
#include <sys/stat.h>
#include <errno.h>
#include <netdb.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cthread_api.h>
#include <Cpool_api.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <Cnetdb.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld.h>
#define RTCPCLD_COMMON
#include <rtcpcld_messages.h>
extern char *getconfent _PROTO((char *, char *, int));

/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

/** Current tape fseq
 */
static int currentTapeFseq = 0;
static void *currentTapeFseqLock = NULL;

/** Lock on list of running file requests (and segments)
 */
static void *runningSegmentLock = NULL;


int rtcpcld_initLocks(
                      tape
                      )
     tape_list_t *tape;
{
  int rc;
  /*
   * Current tape fseq lock
   */
  rc = Cthread_mutex_lock(&currentTapeFseq);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock(currentTapeFseq)");
    return(-1);
  }
  
  rc = Cthread_mutex_unlock(&currentTapeFseq);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock(currentTapeFseq)");
    return(-1);
  }
  currentTapeFseqLock = Cthread_mutex_lock_addr(&currentTapeFseq);
  if ( currentTapeFseqLock == NULL ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_addr(currentTapeFseq)");
    return(-1);
  }

  /*
   * Lock on list of running file requests (and segments)
   */
  rc = Cthread_mutex_lock(tape);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock(tape)");
    return(-1);
  }
  
  rc = Cthread_mutex_unlock(tape);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock(tape)");
    return(-1);
  }
  runningSegmentLock = Cthread_mutex_lock_addr(tape);
  if ( runningSegmentLock == NULL ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_addr(tape)");
    return(-1);
  }

  return(0);

}

static int tapeFseq(
                    newValue
                    )
     int newValue;
{
  int rc = 0, save_serrno;

  save_serrno = serrno;
  rc = Cthread_mutex_lock_ext(currentTapeFseqLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
    return(-1);
  }
  
  if ( currentTapeFseq > 0 ) {
    if ( newValue > 0 ) {
      save_serrno = EINVAL;
      rc = -1;
    } else {
      if ( newValue == 0 ) currentTapeFseq++;
      rc = currentTapeFseq;
    }
  } else {
    currentTapeFseq = newValue;
  }

  rc = Cthread_mutex_unlock_ext(currentTapeFseqLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }

  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

int rtcpcld_setTapeFseq(
                        newValue
                        )
     int newValue;
{
  if ( newValue <= 0 ) {
    serrno = EINVAL;
    return(-1);
  }
  return(tapeFseq(newValue));
}

int rtcpcld_getTapeFseq() 
{
  return(tapeFseq(-1));
}

int rtcpcld_incrementTapeFseq() 
{
  return(tapeFseq(0));
}

int rtcpcld_lockTape() 
{
  int rc;
  rc = Cthread_mutex_lock_ext(runningSegmentLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
    return(-1);
  }
  return(0);
}

int rtcpcld_unlockTape() 
{
  int rc;
  rc = Cthread_mutex_unlock_ext(runningSegmentLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }
  return(0);
}
