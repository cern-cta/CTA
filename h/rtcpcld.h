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

#define ID_TYPE u_signed64
#if defined(CASTOR_STAGER_TAPE_H) && defined(CASTOR_STAGER_SEGMENT_H)
enum NotificationState {
  NOT_NOTIFIED,
  NOTIFIED,
  DONT_NOTIFY
};

typedef struct RtcpcldTapeList 
{
  tape_list_t *tape;
  enum Cstager_TapeStatusCodes_t oldStatus;
  char vwAddress[CA_MAXHOSTNAMELEN+12];
  struct Cstager_Tape_t *tp;
  struct RtcpcldSegmentList *segments;
  struct RtcpcldTapeList *next;
  struct RtcpcldTapeList *prev;
}
RtcpcldTapeList_t;

typedef struct RtcpcldSegmentList 
{
  file_list_t *file;
  int diskFseq;
  enum Cstager_SegmentStatusCodes_t oldStatus;
  enum NotificationState notified;
  struct Cstager_Segment_t *segment;
  struct RtcpcldTapeList *tp;
  struct RtcpcldSegmentList *next;
  struct RtcpcldSegmentList *prev;
}
RtcpcldSegmentList_t;

typedef struct TpReqMap 
{
  tape_list_t *tape;
  int killed;
  RtcpcldTapeList_t *tpList;
  struct TpReqMap *next;
  struct TpReqMap *prev;
}
TpReqMap_t;
#endif /* CASTOR_STAGER_TAPE_H && CASTOR_STAGER_SEGMENT_H */

#if defined(CASTOR_SERVICES_H)
int rtcpcld_getDbSvc _PROTO((
                             struct C_Services_t ***
                             ));
#endif /* CASTOR_SERVICES_H */

#if defined(CASTOR_ISTAGERSVC_H)
int rtcpcld_getStgDbSvc _PROTO((
                                struct Cstager_IStagerSvc_t **
                                ));
#endif /* CASTOR_ISTAGERSVC_H */

int rtcpcld_InitNW  _PROTO((
                            SOCKET **
                            ));
int rtcp_Listen  _PROTO((
                         SOCKET, SOCKET *,
                         int, int
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
int rtcpcld_sendNotifyByAddr _PROTO((
                                     char *,
                                     int
                                     ));
int rtcpcld_notifyRtcpclientd _PROTO((
                                      void
                                      ));

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
int rtcpcld_delTape _PROTO((
                            tape_list_t **
                            ));

int rtcpcld_delSegment _PROTO((
                               file_list_t **
                               ));
int rtcpcld_findTapeKey _PROTO((
                                tape_list_t *,
                                ID_TYPE *
                                ));
void rtcpcld_setTapeKey _PROTO((
                                ID_TYPE
                                ));

int rtcpcld_setVidWorkerAddress _PROTO((
                                        tape_list_t *,
                                        int
                                        ));

#if defined(CASTOR_STAGER_TAPESTATUSCODES_H) && defined(CASTOR_STAGER_SEGMENTSTATUSCODES_H)
int rtcpcld_updateVIDStatus _PROTO((
                                    tape_list_t *, 
                                    enum Cstager_TapeStatusCodes_t,
                                    enum Cstager_TapeStatusCodes_t
                                    ));
int rtcpcld_updateVIDFileStatus _PROTO((
                                        tape_list_t *,
                                        enum Cstager_SegmentStatusCodes_t,
                                        enum Cstager_SegmentStatusCodes_t
                                        ));

int rtcpcld_setFileStatus _PROTO((
                                  rtcpFileRequest_t *,
                                  enum Cstager_SegmentStatusCodes_t,
                                  int
                                  ));
#endif /* CASTOR_STAGER_TAPESTATUSCODES_H && CASTOR_STAGER_SEGMENTSTATUSCODES_H */

int rtcpcld_getPhysicalPath _PROTO((
                                    rtcpTapeRequest_t *,
                                    rtcpFileRequest_t *
                                    ));
int rtcpcld_setVIDFailedStatus _PROTO((
                                      tape_list_t *
                                      ));

char *rtcpcld_fixStr _PROTO((
                             CONST char *
                             ));

#endif /* RTCPCLD_H */
