/******************************************************************************
 *                h/rtcpd_SignalFilePositioned.h
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
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "h/rtcpd_SignalFilePositioned.h"
#include "h/Cthread_api.h"


int rtcpd_SignalFilePositioned(processing_cntl_t *const proc_cntl,
  tape_list_t *const tape, file_list_t *const file) {
    int rc;

    rtcp_log(LOG_DEBUG,"rtcpd_SignalFilePositioned() called\n");
    tl_rtcpd.tl_log( &tl_rtcpd, 11, 2, 
                     "func"   , TL_MSG_PARAM_STR, "rtcpd_SignalFilePositioned",
                     "Message", TL_MSG_PARAM_STR, "called" );
    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    rc = Cthread_mutex_lock_ext(proc_cntl->cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_SignalFilePositioned",
                         "Message", TL_MSG_PARAM_STR, "Cthread_mutex_lock_ext(proc_cntl)",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );        
        return(-1);
    }
    rc = Cthread_cond_broadcast_ext(proc_cntl->cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_cond_broadcast_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_SignalFilePositioned",
                         "Message", TL_MSG_PARAM_STR, "Cthread_cond_broadcast_ext(proc_cntl)",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );                
        (void)Cthread_mutex_unlock_ext(proc_cntl->cntl_lock);
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl->cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
                 sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_SignalFilePositioned",
                         "Message", TL_MSG_PARAM_STR, "Cthread_mutex_unlock_ext(proc_cntl)",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );                        
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_SignalFilePositioned() returns\n");
    tl_rtcpd.tl_log( &tl_rtcpd, 11, 2, 
                     "func"   , TL_MSG_PARAM_STR, "rtcpd_SignalFilePositioned",
                     "Message", TL_MSG_PARAM_STR, "returns" );
    return(0);
}
