/*
 * Copyright (C) 2003 by CERN IT/ADC
 * All rights reserved
 */

/*
 * rtcpcld_messages.h - Remote Tape Copy Client daemon log messages definitions
 */
#ifndef RTCPCLDMESSAGES_H_INCLUDED
#define RTCPCLDMESSAGES_H_INCLUDED

#define RTCPCLIENTD_FACILITY_NAME "rtcpcld"

#define RTCPCLD_NB_PARAMS (4)
#define RTCPCLD_LOG_WHERE "File",DLF_MSG_PARAM_STR,__FILE__,"Line",DLF_MSG_PARAM_INT,__LINE__,"errno", \
    DLF_MSG_PARAM_INT,errno,"serrno",DLF_MSG_PARAM_INT,serrno

enum RtcpcldMessageNo {
    RTCPCLD_MSG_STARTUP,
    RTCPCLD_MSG_INITNW, 
    RTCPCLD_MSG_LISTEN, 
    RTCPCLD_MSG_SYSCALL, 
    RTCPCLD_MSG_NOVOLREQID,
    RTCPCLD_MSG_REQSTARTED,
    RTCPCLD_MSG_PWUID,
    RTCPCLD_MSG_INTERNAL, 
    RTCPCLD_MSG_CALLBACK_POS, 
    RTCPCLD_MSG_CALLBACK_CP, 
    RTCPCLD_MSG_CATALOGUE, 
    RTCPCLD_MSG_VDQM,
    RTCPCLD_MSG_VWFAILED,
    RTCPCLD_MSG_VIDWORKER_STARTED,
    RTCPCLD_MSG_EXTINFO,
    RTCPCLD_MSG_EXTERR,
    RTCPCLD_MSG_VDQMINFO,
    RTCPCLD_MSG_NOREQS,
    RTCPCLD_MSG_CALLBACK_GETW,
    RTCPCLD_MSG_VIDWORKER_ENDED,
    RTCPCLD_MSG_DBSVC,
    RTCPCLD_MSG_WRONG_TAPE,
    RTCPCLD_MSG_FILEREQ,
    RTCPCLD_MSG_SEGMFAILED,
    RTCPCLD_MSG_EXECCMD,
    RTCPCLD_MSG_NEWCONNECT,
    RTCPCLD_MSG_UNKNOPT,
    RTCPCLD_MSG_WAITSEGMS,
    RTCPCLD_MSG_NOMORESEGMS,
    RTCPCLD_MSG_UNEXPECTED_NEWSEGM,
    RTCPCLD_MSG_WAITNEWSEGMS,
    RTCPCLD_MSG_WAITTIMEOUT,
    RTCPCLD_MSG_SEGMCNTS,
    RTCPCLD_MSG_RESERVED32,
    RTCPCLD_MSG_RESERVED33,
    RTCPCLD_MSG_RESERVED34,
    RTCPCLD_MSG_RESERVED35
};

struct RtcpcldMessages {
    enum RtcpcldMessageNo msgNo;
    int severity;
    char messageTxt[CA_MAXLINELEN+1];
};
#if defined(RTCPCLD_COMMON)
struct RtcpcldMessages rtcpcldMessages[] = {
    { RTCPCLD_MSG_STARTUP, DLF_LVL_SYSTEM, "rtcpcld server started"},
    { RTCPCLD_MSG_INITNW, DLF_LVL_ALERT, "rtcpcld_InitNW() failed to initialise network ports"},
    { RTCPCLD_MSG_LISTEN, DLF_LVL_ERROR, "rtcp_Listen() failed"},
    { RTCPCLD_MSG_SYSCALL, DLF_LVL_ERROR, "failed system call"},
    { RTCPCLD_MSG_NOVOLREQID, DLF_LVL_ERROR, "rtcpd did not return VDQM VolReqID"},
    { RTCPCLD_MSG_REQSTARTED, DLF_LVL_SYSTEM, "starting VidWorker  request for VDQM VolReqID"},
    { RTCPCLD_MSG_PWUID, DLF_LVL_ERROR, "rtcopy client daemon UID/GID configuration error"},
    { RTCPCLD_MSG_INTERNAL, DLF_LVL_ERROR, "rtcopy client daemon internal error"},
    { RTCPCLD_MSG_CALLBACK_POS, DLF_LVL_SYSTEM, "rtcopy client daemon callback: file position"},
    { RTCPCLD_MSG_CALLBACK_CP, DLF_LVL_SYSTEM, "rtcopy client daemon callback: file copy"},
    { RTCPCLD_MSG_CATALOGUE, DLF_LVL_ERROR, "catalogue lookup error"},
    { RTCPCLD_MSG_VDQM, DLF_LVL_ERROR, "VDQM call failed"},
    { RTCPCLD_MSG_VWFAILED, DLF_LVL_ERROR, "Failed to start VidWorker"},
    { RTCPCLD_MSG_VIDWORKER_STARTED, DLF_LVL_SYSTEM, "VidWorker started"},
    { RTCPCLD_MSG_EXTINFO, DLF_LVL_DEBUG, "External logger info/debug message"},
    { RTCPCLD_MSG_EXTERR, DLF_LVL_ERROR, "External logger error message"},
    { RTCPCLD_MSG_VDQMINFO, DLF_LVL_SYSTEM, "Request successfully submitted to VDQM"},
    { RTCPCLD_MSG_NOREQS, DLF_LVL_SYSTEM, "Nothing left to do for this VID"},
    { RTCPCLD_MSG_CALLBACK_GETW, DLF_LVL_DEBUG, "rtcopy client daemon callback: get more work"},
    { RTCPCLD_MSG_VIDWORKER_ENDED, DLF_LVL_SYSTEM, "VidWorker ended"},
    { RTCPCLD_MSG_DBSVC, DLF_LVL_ERROR, "Database service error"},
    { RTCPCLD_MSG_WRONG_TAPE, DLF_LVL_ALERT, "Retrieved inconsistent tape request"},
    { RTCPCLD_MSG_FILEREQ, DLF_LVL_SYSTEM, "New file request"},
    { RTCPCLD_MSG_SEGMFAILED, DLF_LVL_ERROR, "Attempt to update segment with SEGMENT_FAILED status "},
    { RTCPCLD_MSG_EXECCMD, DLF_LVL_SYSTEM, "Execute command"},
    { RTCPCLD_MSG_NEWCONNECT, DLF_LVL_SYSTEM, "New connection"},
    { RTCPCLD_MSG_UNKNOPT, DLF_LVL_ERROR, "Unknown option"},
    { RTCPCLD_MSG_WAITSEGMS, DLF_LVL_SYSTEM, "Incomplete segments found. Wait for completion update"},
    { RTCPCLD_MSG_NOMORESEGMS, DLF_LVL_SYSTEM, "No more segments to process"},
    { RTCPCLD_MSG_UNEXPECTED_NEWSEGM, DLF_LVL_ERROR, "Unexpected new segment not updated"},
    { RTCPCLD_MSG_WAITNEWSEGMS, DLF_LVL_SYSTEM, "Wait for new segments"},
    { RTCPCLD_MSG_WAITTIMEOUT, DLF_LVL_SYSTEM, "Waiting for new/incomplete segments timeout"},
    { RTCPCLD_MSG_SEGMCNTS, DLF_LVL_DEBUG,"Segment counts"},
    { RTCPCLD_MSG_RESERVED32, DLF_LVL_DEBUG,""},
    { RTCPCLD_MSG_RESERVED33, DLF_LVL_DEBUG,""},
    { RTCPCLD_MSG_RESERVED34, DLF_LVL_DEBUG,""},
    { RTCPCLD_MSG_RESERVED35, DLF_LVL_DEBUG,""}
};
#else /* RTCPCLD_COMMON */
extern struct RtcpcldMessages rtcpcldMessages;
#endif /* RTCPCLD_COMMON */

#endif /* RTCPCLDMESSAGES_H_INCLUDED */
