/*
 * $Id: stage_protocol.h,v 1.4 2002/03/27 08:08:51 jdurand Exp $
 */

#ifndef __stage_protocol_h
#define __stage_protocol_h

#include "stage_struct.h"
#include "stage_constants.h"
#include "stage_macros.h"

/* ====================================================================== */
/* ====================== DEFINITION OF API PROTOCOL  =================== */
/* ====================================================================== */

/* In the following we distringuish two modes from each marshalling : */
/* a request (INPUT) mode and a full (OUTPUT) mode. This is because */
/* quite a lot of methods, in particular all the methods in stage_iowc */
/* do not need to send the full structure to make a valid request. And */
/* anyway only very few members of structures are used by the stager daemon */
/* to process and validate a request. So, in order not to transfer useless */
/* (might be better to say : not used information), only the relevant members */
/* of the structures are transfered in the INPUT mode. */

#define marshall_STAGE_PATH(magic,from,output,ptr,st) { \
  if (magic >= STGMAGIC3) {                             \
    marshall_LONG(ptr,(st)->reqid);                     \
  }                                                     \
  marshall_STRING(ptr,(st)->upath);                     \
}
#define unmarshall_STAGE_PATH(magic,from,output,ptr,st) { \
  if (magic >= STGMAGIC3) {                               \
    unmarshall_LONG(ptr,(st)->reqid);                     \
  }                                                       \
  output += unmarshall_STRINGN(ptr,(st)->upath,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
}

#ifdef STAGER_SIDE_SERVER_SUPPORT
#define marshall_STAGE_CAT(magic,from,output,ptr,st) {   \
  marshall_LONG(ptr,(st)->reqid);                  \
  marshall_LONG(ptr,(st)->blksize);                \
  marshall_BYTE(ptr,(st)->charconv);               \
  marshall_BYTE(ptr,(st)->keep);                   \
  marshall_LONG(ptr,(st)->lrecl);                  \
  marshall_LONG(ptr,(st)->nread);                  \
  marshall_STRING(ptr,(st)->poolname);             \
  marshall_STRING(ptr,(st)->recfm);                \
  marshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->ipath);              \
  }                                                \
  marshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->group);              \
    marshall_STRING(ptr,(st)->user);               \
    marshall_LONG(ptr,(st)->uid);                  \
    marshall_LONG(ptr,(st)->gid);                  \
    marshall_LONG(ptr,(st)->mask);                 \
    marshall_LONG(ptr,(st)->status);               \
    marshall_HYPER(ptr,(st)->actual_size);         \
    marshall_TIME_T(ptr,(st)->c_time);             \
    marshall_TIME_T(ptr,(st)->a_time);             \
    marshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                \
  switch ((st)->t_or_d) {                          \
  case 't':                                        \
    {                                              \
      int __i_stage_api;                           \
      marshall_STRING(ptr,(st)->u1.t.den);         \
      marshall_STRING(ptr,(st)->u1.t.dgn);         \
      marshall_STRING(ptr,(st)->u1.t.fid);         \
      marshall_BYTE(ptr,(st)->u1.t.filstat);       \
      marshall_STRING(ptr,(st)->u1.t.fseq);        \
      marshall_STRING(ptr,(st)->u1.t.lbl);         \
      marshall_LONG(ptr,(st)->u1.t.retentd);       \
      if (magic >= STGMAGIC4) {                    \
        marshall_LONG(ptr,(st)->u1.t.side);        \
      }                                            \
      marshall_STRING(ptr,(st)->u1.t.tapesrvr);    \
      marshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {    \
        marshall_STRING(ptr,(st)->u1.t.vid[__i_stage_api]); \
        marshall_STRING(ptr,(st)->u1.t.vsn[__i_stage_api]); \
      }                                            \
    }                                              \
    break;                                         \
  case 'd':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    marshall_STRING(ptr,(st)->u1.d.Xparm);         \
    break;                                         \
  case 'a':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    break;                                         \
  case 'm':                                        \
    marshall_STRING(ptr,(st)->u1.m.xfile);         \
  case 'h':                                        \
    marshall_STRING(ptr,(st)->u1.h.xfile);         \
    if (magic <= STGMAGIC2) {                      \
      if (from == STAGE_OUTPUT_MODE) {             \
        marshall_STRING(ptr,(st)->u1.h.server);    \
        marshall_HYPER(ptr,(st)->u1.h.fileid);     \
        marshall_SHORT(ptr,(st)->u1.h.fileclass);  \
      }                                            \
    } else {                                       \
      marshall_STRING(ptr,(st)->u1.h.server);      \
      marshall_HYPER(ptr,(st)->u1.h.fileid);       \
      marshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                              \
    marshall_STRING(ptr,(st)->u1.h.tppool);        \
    if (magic >= STGMAGIC3) {                      \
      marshall_HYPER(ptr,(st)->u1.h.retenp_on_disk); \
      marshall_HYPER(ptr,(st)->u1.h.mintime_beforemigr); \
    }                                              \
    break;                                         \
  default:                                         \
    output = -1;                                   \
    break;                                         \
  }                                                \
}
#else
#define marshall_STAGE_CAT(magic,from,output,ptr,st) {   \
  marshall_LONG(ptr,(st)->reqid);                  \
  marshall_LONG(ptr,(st)->blksize);                \
  marshall_BYTE(ptr,(st)->charconv);               \
  marshall_BYTE(ptr,(st)->keep);                   \
  marshall_LONG(ptr,(st)->lrecl);                  \
  marshall_LONG(ptr,(st)->nread);                  \
  marshall_STRING(ptr,(st)->poolname);             \
  marshall_STRING(ptr,(st)->recfm);                \
  marshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->ipath);              \
  }                                                \
  marshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->group);              \
    marshall_STRING(ptr,(st)->user);               \
    marshall_LONG(ptr,(st)->uid);                  \
    marshall_LONG(ptr,(st)->gid);                  \
    marshall_LONG(ptr,(st)->mask);                 \
    marshall_LONG(ptr,(st)->status);               \
    marshall_HYPER(ptr,(st)->actual_size);         \
    marshall_TIME_T(ptr,(st)->c_time);             \
    marshall_TIME_T(ptr,(st)->a_time);             \
    marshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                \
  switch ((st)->t_or_d) {                          \
  case 't':                                        \
    {                                              \
      int __i_stage_api;                           \
      marshall_STRING(ptr,(st)->u1.t.den);         \
      marshall_STRING(ptr,(st)->u1.t.dgn);         \
      marshall_STRING(ptr,(st)->u1.t.fid);         \
      marshall_BYTE(ptr,(st)->u1.t.filstat);       \
      marshall_STRING(ptr,(st)->u1.t.fseq);        \
      marshall_STRING(ptr,(st)->u1.t.lbl);         \
      marshall_LONG(ptr,(st)->u1.t.retentd);       \
      marshall_STRING(ptr,(st)->u1.t.tapesrvr);    \
      marshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {    \
        marshall_STRING(ptr,(st)->u1.t.vid[__i_stage_api]); \
        marshall_STRING(ptr,(st)->u1.t.vsn[__i_stage_api]); \
      }                                            \
    }                                              \
    break;                                         \
  case 'd':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    marshall_STRING(ptr,(st)->u1.d.Xparm);         \
    break;                                         \
  case 'a':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    break;                                         \
  case 'm':                                        \
    marshall_STRING(ptr,(st)->u1.m.xfile);         \
  case 'h':                                        \
    marshall_STRING(ptr,(st)->u1.h.xfile);         \
    if (magic <= STGMAGIC2) {                      \
      if (from == STAGE_OUTPUT_MODE) {             \
        marshall_STRING(ptr,(st)->u1.h.server);    \
        marshall_HYPER(ptr,(st)->u1.h.fileid);     \
        marshall_SHORT(ptr,(st)->u1.h.fileclass);  \
      }                                            \
    } else {                                       \
      marshall_STRING(ptr,(st)->u1.h.server);      \
      marshall_HYPER(ptr,(st)->u1.h.fileid);       \
      marshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                              \
    marshall_STRING(ptr,(st)->u1.h.tppool);        \
    if (magic >= STGMAGIC3) {                      \
      marshall_HYPER(ptr,(st)->u1.h.retenp_on_disk); \
      marshall_HYPER(ptr,(st)->u1.h.mintime_beforemigr); \
    }                                              \
    break;                                         \
  default:                                         \
    output = -1;                                   \
    break;                                         \
  }                                                \
}
#endif /* STAGER_SIDE_SERVER_SUPPORT */

#ifdef STAGER_SIDE_SERVER_SUPPORT
#define unmarshall_STAGE_CAT(magic,from,output,ptr,st) {   \
  unmarshall_LONG(ptr,(st)->reqid);                  \
  unmarshall_LONG(ptr,(st)->blksize);                \
  unmarshall_BYTE(ptr,(st)->charconv);               \
  unmarshall_BYTE(ptr,(st)->keep);                   \
  unmarshall_LONG(ptr,(st)->lrecl);                  \
  unmarshall_LONG(ptr,(st)->nread);                  \
  output += unmarshall_STRINGN(ptr,(st)->poolname,CA_MAXPOOLNAMELEN+1); \
  output += unmarshall_STRINGN(ptr,(st)->recfm,CA_MAXRECFMLEN+1); \
  unmarshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->ipath,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
  }                                                  \
  unmarshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->group,CA_MAXGRPNAMELEN+1); \
    output += unmarshall_STRINGN(ptr,(st)->user,CA_MAXUSRNAMELEN+1); \
    unmarshall_LONG(ptr,(st)->uid);                  \
    unmarshall_LONG(ptr,(st)->gid);                  \
    unmarshall_LONG(ptr,(st)->mask);                 \
    unmarshall_LONG(ptr,(st)->status);               \
    unmarshall_HYPER(ptr,(st)->actual_size);         \
    unmarshall_TIME_T(ptr,(st)->c_time);             \
    unmarshall_TIME_T(ptr,(st)->a_time);             \
    unmarshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                  \
  switch ((st)->t_or_d) {                            \
  case 't':                                          \
    {                                                \
      int __i_stage_api;                             \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.den,CA_MAXDENLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.dgn,CA_MAXDGNLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fid,CA_MAXFIDLEN+1); \
      UPPER((st)->u1.t.fid);                         \
      unmarshall_BYTE(ptr,(st)->u1.t.filstat);       \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fseq,CA_MAXFSEQLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.lbl,CA_MAXLBLTYPLEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.retentd);       \
      if (magic >= STGMAGIC4) {                      \
        unmarshall_LONG(ptr,(st)->u1.t.side);        \
      }                                              \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.tapesrvr,CA_MAXHOSTNAMELEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {      \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vid[__i_stage_api],CA_MAXVIDLEN+1); \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vsn[__i_stage_api],CA_MAXVSNLEN+1); \
      }                                              \
    }                                                \
    break;                                           \
  case 'd':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.Xparm,23);     \
    break;                                           \
  case 'a':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,STAGE_MAX_HSMLENGTH+1);    \
    break;                                           \
  case 'm':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.m.xfile,STAGE_MAX_HSMLENGTH+1);    \
  case 'h':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.xfile,STAGE_MAX_HSMLENGTH+1);    \
    if (magic <= STGMAGIC2) {                        \
      if (from == STAGE_OUTPUT_MODE) {                 \
        output += unmarshall_STRINGN(ptr,(st)->u1.h.server,CA_MAXHOSTNAMELEN+1); \
        unmarshall_HYPER(ptr,(st)->u1.h.fileid);       \
        unmarshall_SHORT(ptr,(st)->u1.h.fileclass);    \
      }                                                \
    } else {                                         \
      output += unmarshall_STRINGN(ptr,(st)->u1.h.server,CA_MAXHOSTNAMELEN+1); \
      unmarshall_HYPER(ptr,(st)->u1.h.fileid);       \
      unmarshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                                \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.tppool,CA_MAXPOOLNAMELEN+1); \
    if (magic >= STGMAGIC3) {                        \
      unmarshall_HYPER(ptr,(st)->u1.h.retenp_on_disk); \
      unmarshall_HYPER(ptr,(st)->u1.h.mintime_beforemigr); \
    }                                                \
    break;                                           \
  default:                                           \
    output = -1;                                     \
    break;                                           \
  }                                                  \
}
#else
#define unmarshall_STAGE_CAT(magic,from,output,ptr,st) {   \
  unmarshall_LONG(ptr,(st)->reqid);                  \
  unmarshall_LONG(ptr,(st)->blksize);                \
  unmarshall_BYTE(ptr,(st)->charconv);               \
  unmarshall_BYTE(ptr,(st)->keep);                   \
  unmarshall_LONG(ptr,(st)->lrecl);                  \
  unmarshall_LONG(ptr,(st)->nread);                  \
  output += unmarshall_STRINGN(ptr,(st)->poolname,CA_MAXPOOLNAMELEN+1); \
  output += unmarshall_STRINGN(ptr,(st)->recfm,CA_MAXRECFMLEN+1); \
  unmarshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->ipath,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
  }                                                  \
  unmarshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->group,CA_MAXGRPNAMELEN+1); \
    output += unmarshall_STRINGN(ptr,(st)->user,CA_MAXUSRNAMELEN+1); \
    unmarshall_LONG(ptr,(st)->uid);                  \
    unmarshall_LONG(ptr,(st)->gid);                  \
    unmarshall_LONG(ptr,(st)->mask);                 \
    unmarshall_LONG(ptr,(st)->status);               \
    unmarshall_HYPER(ptr,(st)->actual_size);         \
    unmarshall_TIME_T(ptr,(st)->c_time);             \
    unmarshall_TIME_T(ptr,(st)->a_time);             \
    unmarshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                  \
  switch ((st)->t_or_d) {                            \
  case 't':                                          \
    {                                                \
      int __i_stage_api;                             \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.den,CA_MAXDENLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.dgn,CA_MAXDGNLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fid,CA_MAXFIDLEN+1); \
      UPPER((st)->u1.t.fid);                         \
      unmarshall_BYTE(ptr,(st)->u1.t.filstat);       \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fseq,CA_MAXFSEQLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.lbl,CA_MAXLBLTYPLEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.retentd);       \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.tapesrvr,CA_MAXHOSTNAMELEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {      \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vid[__i_stage_api],CA_MAXVIDLEN+1); \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vsn[__i_stage_api],CA_MAXVSNLEN+1); \
      }                                              \
    }                                                \
    break;                                           \
  case 'd':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.Xparm,23);     \
    break;                                           \
  case 'a':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,STAGE_MAX_HSMLENGTH+1);    \
    break;                                           \
  case 'm':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.m.xfile,STAGE_MAX_HSMLENGTH+1);    \
  case 'h':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.xfile,STAGE_MAX_HSMLENGTH+1);    \
    if (magic <= STGMAGIC2) {                        \
      if (from == STAGE_OUTPUT_MODE) {                 \
        output += unmarshall_STRINGN(ptr,(st)->u1.h.server,CA_MAXHOSTNAMELEN+1); \
        unmarshall_HYPER(ptr,(st)->u1.h.fileid);       \
        unmarshall_SHORT(ptr,(st)->u1.h.fileclass);    \
      }                                                \
    } else {                                         \
      output += unmarshall_STRINGN(ptr,(st)->u1.h.server,CA_MAXHOSTNAMELEN+1); \
      unmarshall_HYPER(ptr,(st)->u1.h.fileid);       \
      unmarshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                                \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.tppool,CA_MAXPOOLNAMELEN+1); \
    if (magic >= STGMAGIC3) {                        \
      unmarshall_HYPER(ptr,(st)->u1.h.retenp_on_disk); \
      unmarshall_HYPER(ptr,(st)->u1.h.mintime_beforemigr); \
    }                                                \
    break;                                           \
  default:                                           \
    output = -1;                                     \
    break;                                           \
  }                                                  \
}
#endif /* STAGER_SIDE_SERVER_SUPPORT */

#endif /* __stage_protocol_h */
