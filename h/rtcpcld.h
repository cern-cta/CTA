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
int rtcpcld_updateVIDStatus _PROTO((
                                    tape_list_t *, 
                                    enum TpInfoStatusCodes,
                                    enum TpInfoStatusCodes
                                    ));
int rtcpcld_setFileStatus _PROTO((
                                  rtcpFileRequest_t *,
                                  enum TpFileInfoStatusCodes
                                  ));
int rtcpcld_getPhysicalPath _PROTO((
                                    rtcpTapeRequest_t *,
                                    rtcpFileRequest_t *
                                    ));


#endif /* RTCPCLD_H */
