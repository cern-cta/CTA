/******************************************************************************
 *                      rtcpcldapi.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: rtcpcldapi.h,v $ $Revision: 1.4 $ $Release$ $Date: 2004/06/21 07:33:20 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

/** RTCOPY client daemon API
 *
 * \file rtcpcldapi.h
 *
 * The RTCOPY client daemon API provides an interface similar to rtcpapi.h for
 * submitting tape mover requests. The difference is that rtcpcldapi passes the
 * request to the stager request database using the Cstager_Tape and Cstager_Segment
 * APIs. Apart for an unreliable (UDP) notification mechanism there are no
 * network connections between the rtcpcldapi client and the RTCOPY subsystem. All
 * request updates are synchronized through the databse.
 *
 * <P>
 * Only a subset of the rtcpTapeRequest_t and rtcpFileRequest_t fields are passed
 * through the database. All other fields are set to default values and any other
 * value set by client is ignored. The relevant fields in rtcpTapeRequest_t are:
 *  - <b>vid</b>
 *  - <b>side</b>
 *  - <b>mode</b>
 *
 * The relevant fields in rtcpFileRequest_t are:
 *  - <b>tape_fseq</b>
 *  - <b>blockid</b>
 *  - <b>bytes_in</b>
 *  - <b>bytes_out</b>
 *  - <b>offset</b>
 *  - <b>host_bytes</b>
 *  - <b>file_path</b>
 *  - <b>castorSegAttr.segmCksumAlgorithm</b>
 *  - <b>castorSegAttr.segmCksum</b>
 *  - <b>castorSegAttr.nameServerHostName</b>
 *  - <b>castorSegAttr.castorFileId</b>
 *  - <b>stgReqId (UUID)</b>
 *  - <b>err.errorcode</b>
 *  - <b>err.severity</b>
 *  - <b>err.errmsgtxt</b>
 *
 * The following fields:
 *  - <b>proc_status</b>
 *  - <b>cprc</b>
 *  - <b>stageID</b>
 *  - <b>stageSubreqID</b>
 *
 * are used internally in rtcpcldapi.h but not passed on to the RTCOPY server.
 *
 */

#ifndef H_RTCPCLDAPI_H 
#define H_RTCPCLDAPI_H 1
#include <Castor_limits.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>

/** Client callback before and after each file copy
 *
 * \param tapereq Tape request structure
 * \param filereq File request structure
 *
 * The callback is identical to the rtcp_ClientCallback()
 * provided with rtcpc(). The client is given control through
 * this entry after each tape position and file copy. The
 * difference to rtcpc() is that since rtcpcldc() is not
 * communicating directly with the tape mover, the callback
 * is asynchronous.
 *
 * \see rtcpc(3)
 */
EXTERN_C int (*rtcpcld_ClientCallback) _PROTO((
                                               rtcpTapeRequest_t *tapereq,
                                               rtcpFileRequest_t *filereq
                                               ));
/** Client callback to fill incomplete file requests
 *
 * \param tapereq Tape request structure
 * \param filereq File request structure
 *
 *<P>
 *Give control to caller when an incomplete (missing tape position or disk
 *path) file request has been encountered. The caller is supposed to complete
 *the request in this callback.
 */
EXTERN_C int (*rtcpcld_MoreInfoCallback) _PROTO((
                                                 rtcpTapeRequest_t *tapereq,
                                                 rtcpFileRequest_t *filereq
                                                 ));
/** RTCOPY client daemon client
 *
 * \param tape - double linked list specifying the entire remote tape copy request.
 *
 *<P>
 *Provides an interface similar to rtcpc() for interacting with the RTCOPY client
 *daemon (rtcpclientd). The difference to rtcpc() is that rtcpcldc() does not
 *have any direct link to the daemon like rtcpc() has with the rtcpd mover process on
 *the tape servers. Instead rtcpcldc() interacts with the rtcpclientd through the
 *stager catalogue database using the Cstager_Tape and Cstager_Segment interface
 *defined in castor/stager/Tape.h and castor/stager/Segment.h. The rtcpclientd takes
 *all its requests from the stager catalogue database.
 *
 *The rtcpcldc() can be regarded as an example how to insert and track the evolution
 *of tape requests in the stager catalogue. It emulates the functionality of the rtcpc()
 *interface and attempts to preserve the same error code and severity mapping. Part of
 *the emulation is also the rtcpcld_ClientCallback() interface that the caller can use
 *to regain control before and after each file copy. This is similar to the 
 *rtcpc_ClientCallback() interface associated with rtcpc(). In addition a second callback
 *entry point, rtcpcld_MoreInfoCallback(), has been defined for requesting the caller
 *to fill missing information (e.g. disk path or file sequence number) to incomplete
 *file request.
 *
 * \return 
 *  - 0 == OK
 *  - -1 if an error occurred. The serrno global is set as for rtcpc().
 *
 * \see rtcpc(3)
 */
int rtcpcldc _PROTO((
                     tape_list_t *tape
                     ));
/** Cleanup after a series of rtcpcldc_appendFileReqs() calls
 *
 * \param tape - tape list passed in the original rtcpcldc() call.
 *
 *<P>
 *The rtcpcldc_cleanup() cleans up the database service connection and internal
 *memory allocations associated with the rtcpcldc() and rtcpcldc_appendFileReqs()
 *calls. It is only necessary to call rtcpcldc_cleanup() if file requests have
 *been appended from a thread different from the one where rtcpcldc() is running.
 *The tape parameter may be NULL, in which case only the database service
 *connection (if any) used by this thread is cleaned up. There is no need to
 *call rtcpclc_cleanup after a call to rtcpcldc() but it will not cause any
 *harm.
 */
void rtcpcldc_cleanup _PROTO((
                              tape_list_t *tape
                              ));
/** Append files to a running request
 *
 * \param tape tape request to which the files should be appended.
 * \param file files to be appended.
 *
 *<P>
 *rtcpcldc_appendFileReqs() allows for appending files to a running (or queued)
 *RTCOPY client daemon request. The passed file requests will be appended to
 *the original request and the corresponding segments in the stager catalogue
 *will be created. The tape parameter should be the same as the one passed in the
 *original rtcpcldc() call. The tape request is locked internally to avoid race
 *conditions if rtcpcldc() and rtcpcldc_appendFileReqs() are called from different
 *threads.
 *
 * \return
 *  - 0 == OK
 *  - -1 if an error occurred. The serrno global is set to one of the following values:
 *    - <B>EINVAL</B> tape or file arguments are NULL
 *    - <B>SESYSERR</B> memory allocation (calloc()) failed
 *    - <B>ENOENT</B> the tape argument is not a running (or queued) tape request
 *    - Other error codes may be set in Cmutex, Cglobals or C_Services interfaces
 *.
 *
 * \see Cmutex, Cglobals man-pages
 */
int rtcpcldc_appendFileReqs _PROTO((
                                    tape_list_t *tape,
                                    file_list_t *file
                                    ));
#endif /* H_RTCPCLDAPI_H */
