/*
 * Copyright (C) 2003 by CERN IT-ADC
 * All rights reserved
 */


/*
 * rtcpcld.h,v 1.5 2004/04/30 14:25:36 CERN IT-ADC Olof Barring
 */

/*
 * rtcpcld.h - rtcopy client daemon specific definitions
 */

#ifndef RTCPCLD_H
#define RTCPCLD_H

typedef struct RtcpcldVIDChild {
    char vid[CA_MAXVIDLEN+1];
    int side;
    int fseq;
    unsigned char blockid[4];
    char fid[CA_MAXFIDLEN+1];
    int childPid;
    int childPort;
    struct RtcpcldVIDChild *next;
    struct RtcpcldVIDChild *prev;
} RtcpcldVIDChild_t;

#if defined(CASTOR_STAGER_TAPE_H) && defined(CASTOR_STAGER_SEGMENT_H)
#define ID_TYPE unsigned long

typedef struct RtcpcldTapeList 
{
  tape_list_t *tape;
  struct Cstager_Tape_t *tp;
  struct RtcpcldSegmentList *segments;
  struct RtcpcldTapeList *next;
  struct RtcpcldTapeList *prev;
}
RtcpcldTapeList_t;

typedef struct RtcpcldSegmentList 
{
  file_list_t *file;
  struct Cstager_Segment_t *segment;
  struct RtcpcldTapeList *tp;
  struct RtcpcldSegmentList *next;
  struct RtcpcldSegmentList *prev;
}
RtcpcldSegmentList_t;
#endif /* CASTOR_STAGER_TAPE_H && CASTOR_STAGER_SEGMENT_H */

int rtcpcld_InitNW  _PROTO((
                            SOCKET **
                            ));
int rtcp_Listen  _PROTO((
                         SOCKET, SOCKET *,
                         int
                         ));
int rtcp_CleanUp  _PROTO((
                          SOCKET **, 
                          int
                          ));
int rtcpcld_Listen _PROTO((
                           SOCKET *, 
                           SOCKET *, 
                           int
                           ));
int rtcpcld_initLogging _PROTO((
                                char *
                                ));
void rtcpcld_extlog _PROTO((
                            int,
                            CONST char *,
                            ...
                            ));
int rtcpcld_initNotify _PROTO((
                               SOCKET **
                               ));
int rtcpcld_initNotifyByPort _PROTO((
                                     SOCKET **,
                                     int *
                                     ));
int rtcpcld_getNotifyWithAddr _PROTO((
                                      SOCKET,
                                      char *
                                      ));
int rtcpcld_getNotify _PROTO((
                              SOCKET
                              ));
int rtcpcld_sendNotify _PROTO((
                               char *
                               ));
int rtcpcld_getNotify _PROTO((SOCKET));
int rtcpcld_getVIDsToDo _PROTO((
                                tape_list_t ***,
                                int *
                                ));
int rtcpcld_getReqsForVID _PROTO((
                                  tape_list_t *
                                  ));
int rtcpcld_anyReqsForVID _PROTO((
                                  tape_list_t *
                                  ));

#if defined(CASTOR_STAGER_TAPE_H) && defined(CASTOR_STAGER_SEGMENT_H)
int rtcpcld_updateVIDStatus _PROTO((
                                    tape_list_t *, 
                                    enum Cstager_TapeStatusCodes_t,
                                    enum Cstager_TapeStatusCodes_t
                                    ));
int rtcpcld_setFileStatus _PROTO((
                                  rtcpFileRequest_t *,
                                  enum Cstager_SegmentStatusCodes_t
                                  ));
#endif /* CASTOR_STAGER_TAPE_H) && CASTOR_STAGER_SEGMENT_H */

int rtcpcld_getPhysicalPath _PROTO((
                                    rtcpTapeRequest_t *,
                                    rtcpFileRequest_t *
                                    ));


#endif /* RTCPCLD_H */
