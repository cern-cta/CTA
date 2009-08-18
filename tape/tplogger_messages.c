/******************************************************************************************************
 *                                                                                                    *
 * tplogger_messages.c - Castor Tape Logger Messages                                                  *
 * Copyright (C) 2006 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/


/*
** $Id: tplogger_messages.c,v 1.12 2009/08/18 09:43:02 waldron Exp $
*/

#include <string.h>

#include "tplogger_api.h"

/**
 * @file  tplogger_messages.c
 * @brief predefined messages for taped; mainly taken from h/Ctape.h
 *        predefined messages for rtcpd; mainly taken from h/rtcp_constants.h
 */


/*
** The messages array.
*/
tplogger_message_t tplogger_messages[] = {

        {   0, TL_LVL_ERROR,  "TP000 - Dummy" }
};


/*
** The predefined messages for rtcpd/taped/rmcd
*/
tplogger_message_t tplogger_messages_tpdaemon[] = {

        /* original tape messages */
        {   0, TL_LVL_ERROR,  "TP000 - tape daemon not available"               },  /* host      */
        {   1, TL_LVL_ERROR,  "TP001 - no response from tape daemon"            },
        {   2, TL_LVL_ERROR,  "TP002 - error"                                   },  /* error     */
        {   3, TL_LVL_ERROR,  "TP003 - illegal function"                        },  /* function  */
        {   4, TL_LVL_ERROR,  "TP004 - error getting request"                   },  /* netread   */
        {   5, TL_LVL_ERROR,  "TP005 - cannot get memory"                       },
        {   6, TL_LVL_ERROR,  "TP006 - invalid value"                           },  /* value name*/
        {   7, TL_LVL_ERROR,  "TP007 - fid is mandatory when TPPOSIT_FID"       },
        {   8, TL_LVL_ERROR,  "TP008 - not accessible"                          },  /* what      */
        {   9, TL_LVL_ERROR,  "TP009 - could not configure"                     },  /* what, why */
        {  10, TL_LVL_ERROR,  "TP010 - resources already reserved for this job" },
        {  11, TL_LVL_ERROR,  "TP011 - too many tape users"                     },
        {  12, TL_LVL_ERROR,  "TP012 - too many drives requested"               },
        {  13, TL_LVL_ERROR,  "TP013 - invalid device group name"               },
        {  14, TL_LVL_ERROR,  "TP014 - reserve not done"                        },
        {  15, TL_LVL_ERROR,  "TP015 - drive with specified name/characteristics does not exist" },
        {  16, TL_LVL_ERROR,  "TP016 - "                                        },
        {  17, TL_LVL_ERROR,  "TP017 - cannot use blp in write mode"            },
        {  18, TL_LVL_ERROR,  "TP018 - duplicate option"                        },  /* option    */
        {  19, TL_LVL_ERROR,  "TP019 - "                                        },
        {  20, TL_LVL_MONITORING,  "TP020 - mount tape"                         },  /* vid       */
                                                                                    /* labels    */
                                                                                    /* rings     */
                                                                                    /* drive     */
                                                                                    /* host      */
                                                                                    /* user      */
                                                                                    /* pid       */
                                                                                    /* why       */
        {  21, TL_LVL_ERROR,  "TP021 - label type mismatch"                     },  /* request   */
                                                                                    /* tape      */
        {  22, TL_LVL_ERROR,  "TP022 - path exists already"                     },
        {  23, TL_LVL_ERROR,  "TP023 - mount cancelled by operator"             },  /* operator  */
        {  24, TL_LVL_ERROR,  "TP024 - file does not exist"                     },  /* file      */
        {  25, TL_LVL_ERROR,  "TP025 - bad label structure"                     },
        {  26, TL_LVL_ERROR,  "TP026 - system error"                            },
        {  27, TL_LVL_ERROR,  "TP027 - you are not registered in account file"  },
        {  28, TL_LVL_ERROR,  "TP028 - file not expired"                        },
        {  29, TL_LVL_ERROR,  "TP029 - pathname is mandatory"                   },
        {  30, TL_LVL_ERROR,  "TP030 - I/O error"                               },
        {  31, TL_LVL_ERROR,  "TP031 - no vid specified"                        },
        {  32, TL_LVL_ERROR,  "TP032 - config file"                             },  /* line      */
                                                                                    /* file      */
        {  33, TL_LVL_ERROR,  "TP033 - Drive not operational" },                    /* drive     */
                                                                                    /* host      */
        {  34, TL_LVL_ERROR,  "TP034 - all entries for a given device group must be grouped together" },
        {  35, TL_LVL_ERROR,  "TP035 - configuring"                             },  /* drive     */
                                                                                    /* up/down?  */
        {  36, TL_LVL_ERROR,  "TP036 - path is mandatory when rls flags is"     },  /* flags     */
        {  37, TL_LVL_ERROR,  "TP037 - path does not exist"                     },  /* path      */
        {  38, TL_LVL_ERROR,  "TP038 - pathname too long"                       },
        {  39, TL_LVL_ERROR,  "TP039 - vsn mismatch"                            },  /* request   */
                                                                                    /* tape vsn  */

        {  40, TL_LVL_ERROR,  "TP040 - release pending"                         },
        {  41, TL_LVL_ERROR,  "TP041 - failure"                                 },  /* action    */
                                                                                    /* cur_vid   */
                                                                                    /* cur_unm   */
                                                                                    /* why       */
        {  42, TL_LVL_ERROR,  "TP042 - error"                                   },  /* what/why  */
                                                                                    /* what/why  */
                                                                                    /* what/why  */

        {  43, TL_LVL_ERROR,  "TP043 - configuration line too long"             },  /* line      */
        {  44, TL_LVL_ERROR,  "TP044 - fid mismatch: request"                   },  /* request   */
                                                                                    /* file      */
                                                                                    /* tape fid  */
        {  45, TL_LVL_ERROR,  "TP045 - cannot write file"                       },  /* file      */
                                                                                    /* no_files  */
        {  46, TL_LVL_ERROR,  "TP046 - request too large"                       },  /* max       */
        {  47, TL_LVL_ERROR,  "TP047 - reselect server requested by operator"   },
        {  48, TL_LVL_WARNING, "TP048 - config postponed"                       },  /* curr_ass  */
        {  49, TL_LVL_ERROR,  "TP049 - option IGNOREEOI is not valid for label type al or sl" },
        {  50, TL_LVL_ERROR,  "TP050 - vid mismatch"},                              /* req_vid   */
                                                                                    /* drv_vid   */
        {  51, TL_LVL_ERROR,  "TP051 - fatal configuration error"               },  /* what      */
                                                                                    /* why       */
        {  52, TL_LVL_ERROR,  "TP052 - WSAStartup unsuccessful"                 },
        {  53, TL_LVL_ERROR,  "TP053 - you are not registered in the unix group/passwd mapping file" },
        {  54, TL_LVL_ERROR,  "TP054 - tape not mounted or not ready"           },
        {  55, TL_LVL_ERROR,  "TP055 - parameter inconsistency"                 },  /* vid       */
                                                                                    /* par_req   */
                                                                                    /* par_TMS   */

        {  56, TL_LVL_MONITORING, "TP056 - request"                             },  /* type      */
                                                                                    /* uid       */
                                                                                    /* gid       */
                                                                                    /* clienthost*/
        {  57, TL_LVL_ERROR,  "TP057 - drive is not free"                       },  /* drive     */


        {  58, TL_LVL_ERROR,  "TP058 - no free drive"                           },
        {  59, TL_LVL_ERROR,  "TP059 - invalid reason"                          },
        {  60, TL_LVL_ERROR,  "TP060 - invalid combination of method and filstat" },
        {  61, TL_LVL_ERROR,  "TP061 - filstat value incompatible with read-only mode" },
        {  62, TL_LVL_ERROR,  "TP062 - tape to be prelabelled"                  },  /* tape      */
                                                                                    /* what      */
                                                                                    /* what      */
        {  63, TL_LVL_ERROR,  "TP063 - invalid user"                            },  /* user      */
        {  64, TL_LVL_ERROR,  "TP064 - invalid method for this label type"      },
        {  65, TL_LVL_ERROR,  "TP065 - tape has bad MIR"                        },  /* tape      */
                                                                                    /* labels    */
                                                                                    /* drive     */
                                                                                    /* host      */
                                                                                    /* name      */
                                                                                    /* pid       */
        /* typical usage cycle, exclusive use of level IMPORTANT */
        {  70, TL_LVL_IMPORTANT,  "TP070 - Drive put up"                        },
        {  71, TL_LVL_IMPORTANT,  "TP071 - Reserve request"                     },
        {  72, TL_LVL_IMPORTANT,  "TP072 - Mount request"                       },
        {  73, TL_LVL_IMPORTANT,  "TP073 - Tape mounted"                        },
        {  74, TL_LVL_IMPORTANT,  "TP074 - Release request"                     },
        {  75, TL_LVL_IMPORTANT,  "TP075 - Tape released"                       },
        {  76, TL_LVL_IMPORTANT,  "TP076 - Free drive request"                  },
        {  77, TL_LVL_IMPORTANT,  "TP077 - Drive put down"                      },
        {  78, TL_LVL_IMPORTANT,  "TP078 - Put tape into drive (TP020)"         },
        {  79, TL_LVL_IMPORTANT,  "TP079 - Remove tape from drive"              },

        /* tape alerts */
        {  80, TL_LVL_ERROR,      "TP080 - Tape alerts error"                   },
        {  81, TL_LVL_WARNING,    "TP081 - Tape alerts no data"                 },
        {  82, TL_LVL_MONITORING, "TP082 - Tape alerts info"                    },

        /* bad MIR */
        {  85, TL_LVL_MONITORING, "TP085 - Bad MIR (general)"                   },
        {  86, TL_LVL_WARNING,    "TP086 - Bad MIR detected"                    },
        {  87, TL_LVL_ERROR,      "TP087 - Bad MIR request canceled"            },
        {  88, TL_LVL_WARNING,    "TP088 - Bad MIR request continued"           },
        {  89, TL_LVL_ERROR,      "TP089 - Bad MIR repair failed"               },
        {  90, TL_LVL_MONITORING, "TP090 - Bad MIR repair finished"             },

        {  91, TL_LVL_IMPORTANT,  "TP091 - Mount summary"                       },
        {  92, TL_LVL_IMPORTANT,  "TP092 - Unmount summary"                     },

        /* general messages */
        { 101, TL_LVL_EMERGENCY , "TP101 - General Emergency Message"           },
        { 102, TL_LVL_ALERT     , "TP102 - General Alert Message"               },
        { 103, TL_LVL_ERROR     , "TP103 - General Error Message"               },
        { 104, TL_LVL_WARNING   , "TP104 - General Warning Message"             },
        { 105, TL_LVL_AUTH      , "TP105 - General Auth Message"                },
        { 106, TL_LVL_SECURITY  , "TP106 - General Security Message"            },
        { 107, TL_LVL_USAGE     , "TP107 - General Usage Message"               },
        { 108, TL_LVL_SYSTEM    , "TP108 - General System Message"              },
        { 109, TL_LVL_IMPORTANT , "TP109 - General Important Message"           },
        { 110, TL_LVL_MONITORING, "TP110 - General Monitoring Message"          },
        { 111, TL_LVL_DEBUG     , "TP111 - General Debug Message"               }
};

tplogger_message_t tplogger_messages_rtcpd[] = {

        /* general messages */
        {   1, TL_LVL_EMERGENCY , "RT001 - General Emergency Message"           },
        {   2, TL_LVL_ALERT     , "RT002 - General Alert Message"               },
        {   3, TL_LVL_ERROR     , "RT003 - General Error Message"               },
        {   4, TL_LVL_WARNING   , "RT004 - General Warning Message"             },
        {   5, TL_LVL_AUTH      , "RT005 - General Auth Message"                },
        {   6, TL_LVL_SECURITY  , "RT006 - General Security Message"            },
        {   7, TL_LVL_USAGE     , "RT007 - General Usage Message"               },
        {   8, TL_LVL_SYSTEM    , "RT008 - General System Message"              },
        {   9, TL_LVL_IMPORTANT , "RT009 - General Important Message"           },
        {  10, TL_LVL_MONITORING, "RT010 - General Monitoring Message"          },
        {  11, TL_LVL_DEBUG     , "RT011 - General Debug Message"               },

        /* typical usage cycle, exclusive use of level IMPORTANT */
        {  20, TL_LVL_IMPORTANT , "RT020 - Connection from authorized host"     },
        {  21, TL_LVL_IMPORTANT , "RT021 - Assigned request"                    },
        {  22, TL_LVL_IMPORTANT , "RT022 - CPDSKTP request"                     },
        {  23, TL_LVL_IMPORTANT , "RT023 - CPTPDSK request"                     },
        {  24, TL_LVL_IMPORTANT , "RT024 - DUMP request"                        },
        {  25, TL_LVL_IMPORTANT , "RT025 - Tape used"                           },
        {  26, TL_LVL_IMPORTANT , "RT026 - File used"                           },
        {  27, TL_LVL_IMPORTANT , "RT027 - Disk file opened"                    },
        {  28, TL_LVL_IMPORTANT , "RT028 - Tape reserve request"                },
        {  29, TL_LVL_IMPORTANT , "RT029 - Tape reserved"                       },
        {  30, TL_LVL_IMPORTANT , "RT030 - Tape mount request"                  },
        {  31, TL_LVL_IMPORTANT , "RT031 - Tape mounted"                        },
        {  32, TL_LVL_IMPORTANT , "RT032 - Tape position request"               },
        {  33, TL_LVL_IMPORTANT , "RT033 - Tape positioned"                     },
        {  34, TL_LVL_IMPORTANT , "RT034 - Tape file opened"                    },
        {  35, TL_LVL_IMPORTANT , "RT035 - Tape file closed"                    },
        {  36, TL_LVL_IMPORTANT , "RT036 - Tape release request"                },
        {  37, TL_LVL_IMPORTANT , "RT037 - Tape released"                       },
        {  38, TL_LVL_IMPORTANT , "RT038 - Disk file closed"                    },
        {  39, TL_LVL_DEBUG     , "RT039 - Waiting time"                        },
        {  40, TL_LVL_DEBUG     , "RT040 - Service time"                        },
        {  41, TL_LVL_DEBUG     , "RT041 - Data transfer rate"                  },
        {  42, TL_LVL_IMPORTANT , "RT042 - Request successful"                  },
        {  43, TL_LVL_ERROR     , "RT043 - Request failed"                      },
        {  44, TL_LVL_IMPORTANT , "RT044 - Request statistics"                  },

        { 101, TL_LVL_ERROR     , "RT101 - -qn OPTION INVALID FOR CPTPDSK"      },  /* obsolete  */
        { 102, TL_LVL_ERROR     , "RT102 - ABORT BECAUSE INVALID OPTION"        },  /* obsolete  */
        { 103, TL_LVL_ERROR     , "RT103 - BLOCK REQUESTED SMALLER THAN BLOCK ON TAPE" },
                                                                                   /* block no  */
        { 104, TL_LVL_ERROR     , "RT104 - BLOCK SIZE CANNOT BE EQUAL TO ZERO"  },
        { 105, TL_LVL_ERROR     , "RT105 - BUFFER ALLOCATION ERROR"             },
        { 106, TL_LVL_ERROR     , "RT106 - DISK FILE FORMAT NOT UNFORMATTED SEQUENTIAL FORTRAN" },
        { 107, TL_LVL_ERROR     , "RT107 - DISK FILE PATHNAMES MUST BE SPECIFIED" },
        { 108, TL_LVL_ERROR     , "RT108 - ERROR CLOSING DISK FILE"             },
        { 109, TL_LVL_ERROR     , "RT109 - ERROR GETTING DEVICE STATUS"         },
        { 110, TL_LVL_ERROR     , "RT110 - ERROR OPENING DISK FILE"             },
        { 111, TL_LVL_ERROR     , "RT111 - ERROR OPENING TAPE FILE"             },
        { 112, TL_LVL_ERROR     , "RT112 - ERROR READING DISK FILE"             },
        { 113, TL_LVL_ERROR     , "RT113 - ERROR READING FROM TAPE"             },  /* error     */
                                                                                    /* block no  */
        { 114, TL_LVL_ERROR     , "RT114 - ERROR SETTING LIST TAPE I/O"         },  /* error     */
        { 115, TL_LVL_ERROR     , "RT115 - ERROR WRITING ON DISK"               },  /* error     */
        { 116, TL_LVL_ERROR     , "RT116 - ERROR WRITING ON TAPE"               },  /* error     */
                                                                                    /* block no  */
        { 117, TL_LVL_ERROR     , "RT117 - ERROR WRITING ON TAPE"               },  /* erreg     */
                                                                                    /* block no  */
        { 118, TL_LVL_ERROR     , "RT118 - ERROR, BLANK CHECK"                  },  /* block no  */
        { 119, TL_LVL_ERROR     , "RT119 - ERROR, BLOCK TOO LARGE OR TOO SMALL" },  /* block no  */
        { 120, TL_LVL_ERROR     , "RT120 - ERROR, DISK FILE NAME MISSING"       },
        { 121, TL_LVL_ERROR     , "RT121 - ERROR, DISK FILES ARE EMPTY"         },
        { 122, TL_LVL_ERROR     , "RT122 - ERROR, NEXTVOL INTERRUPTED"          },
        { 123, TL_LVL_ERROR     , "RT123 - ERROR, RECORD LENGTH GREATER THAN SPECIFIED SIZE" },
                                                                                    /* size      */
        { 124, TL_LVL_ERROR     , "RT124 - ERROR, TAPE VOLUMES OVERFLOW"        },
        { 125, TL_LVL_ERROR     , "RT125 - I/O ERROR READING FROM TAPE"         },  /* error     */
                                                                                    /* block no  */
        { 126, TL_LVL_ERROR     , "RT126 - I/O ERROR WRITING ON TAPE"           },  /* error     */
                                                                                    /* block no  */
        { 127, TL_LVL_ERROR     , "RT127 - INCORRECT NUMBER OF FILENAMES SPECIFIED" },
                                                                                    /* error     */
        { 128, TL_LVL_ERROR     , "RT128 - INCORRECT OR MISSING TRAILER LABEL ON TAPE" },
        { 129, TL_LVL_ERROR     , "RT129 - INVALID BLOCK SIZE SPECIFIED"        },
        { 130, TL_LVL_ERROR     , "RT130 - INVALID FORMAT SPECIFIED"            },
        { 131, TL_LVL_ERROR     , "RT131 - INVALID LABEL TYPE"                  },  /* lbl type  */
        { 132, TL_LVL_ERROR     , "RT132 - INVALID MAX. SIZE OF FILE SPECIFIED" },
        { 133, TL_LVL_ERROR     , "RT133 - INVALID NUMBER OF RECORDS SPECIFIED" },
        { 134, TL_LVL_ERROR     , "RT134 - INVALID OPTION FOR -E"               },  /* option    */
        { 135, TL_LVL_ERROR     , "RT135 - INVALID RECORD LENGTH SPECIFIED"     },
        { 136, TL_LVL_ERROR     , "RT136 - NETWORK ERROR DURING RFIO OPERATION" },
        { 137, TL_LVL_ERROR     , "RT137 - PARITY ERROR OR BLANK TAPE"          },  /* block no  */
        { 138, TL_LVL_ERROR     , "RT138 - RECORD LENGTH CAN'T BE GREATER THAN BLOCK SIZE"},
                                                                                   /* rec len   */
                                                                                   /* blk size  */
        { 139, TL_LVL_ERROR     , "RT139 - RECORD LENGTH HAS TO BE SPECIFIED"   },
        { 140, TL_LVL_ERROR     , "RT140 - TAPE DEVICE NOT OPERATIONAL"         },
        { 141, TL_LVL_ERROR     , "RT141 - TAPE IS NOW INCORRECTLY TERMINATED"  },
        { 142, TL_LVL_ERROR     , "RT142 - THE FILE SEQUENCE LIST IS NOT VALID" },
        { 143, TL_LVL_ERROR     , "RT143 - TRAILING MINUS IN -q SEQUENCE LIST IS INVALID FOR CPDSKTP" },
        { 144, TL_LVL_ERROR     , "RT144 - VSN OR VID MUST BE SPECIFIED"        },
        { 145, TL_LVL_ERROR     , "RT145 - CANNOT TRANSFER DATA TO AN AFS BASED FILE" },
        { 146, TL_LVL_ERROR     , "RT146 - INTERNAL ERROR ON STARTUP SIZES"     },
        { 147, TL_LVL_ERROR     , "RT147 - ALREADY APPENDED"                    }, /* kB2File   */
                                                                                   /* limit     */
        { 148, TL_LVL_ERROR     , "RT148 - INVALID BUFFER SIZE SPECIFIED. CHECK BLOCK SIZE AND RECORD LENGTH!"},
                                                                                   /* spec size */
        { 149, TL_LVL_ERROR     , "RT149 - SPECIFIED lrecl DOES NOT CORRESPOND TO f77 lrecl" },
                                                                                   /* lrecl     */
                                                                                   /* f77 lrecl */
        { 150, TL_LVL_ERROR     , "RT150 - DISK FILE MODIFIED DURING REQUEST"   },
        { 151, TL_LVL_ERROR     , "RT151 - TAPE FILES MUST BE IN SEQUENCE"      },
        { 152, TL_LVL_ERROR     , "RT152 - DISK FILE CANNOT BE A CASTOR HSM FILE" },
        { 153, TL_LVL_ERROR     , "RT153 - INVALID SETTING FOR CONCAT_TO_EOD FLAG" },
        { 154, TL_LVL_ERROR     , "RT154 - INVALID SETTING FOR NOCONCAT_TO_EOD FLAG" },

        { 201, TL_LVL_MONITORING, "RT201 - BYTES COPIED"                        },
        { 202, TL_LVL_MONITORING, "RT202 - RECORDS COPIED"                      },
        { 203, TL_LVL_MONITORING, "RT203 - APPEND MODE"                         },
        { 204, TL_LVL_MONITORING, "RT204 - BLOCK SIZE"                          },  /* blk size   */
        { 205, TL_LVL_MONITORING, "RT205 - END OF FILE"                         },
        { 206, TL_LVL_MONITORING, "RT206 - END OF TRANSFER"                     },
        { 207, TL_LVL_MONITORING, "RT207 - MAX. NB OF RECORDS"                  },
        { 208, TL_LVL_MONITORING, "RT208 - MAX. SIZE OF FILE"                   },  /* max size   */
        { 209, TL_LVL_MONITORING, "RT209 - RECORD FORMAT"                       },  /* format     */
        { 210, TL_LVL_MONITORING, "RT210 - RECORD LENGTH"                       },  /* length     */
        { 211, TL_LVL_MONITORING, "RT211 - SKIPPING TO NEXT BLOCK"              },
        { 212, TL_LVL_MONITORING, "RT212 - FILE TRUNCATED"                      },
        { 213, TL_LVL_MONITORING, "RT213 - CONVERTING DISK DATA FROM ASCII TO EBCDIC" },
        { 214, TL_LVL_MONITORING, "RT214 - CONVERTING TAPE DATA FROM EBCDIC TO ASCII" },
        { 215, TL_LVL_MONITORING, "RT215 - KBYTES SENT BY HOST"                 },  /* kbytes     */
        { 216, TL_LVL_MONITORING, "RT216 - KBYTES WRITTEN TO TAPE"              },  /* kbytes     */
        { 217, TL_LVL_MONITORING, "RT217 - COMPRESSION RATE ON TAPE"            },  /* rate       */
        { 218, TL_LVL_MONITORING, "RT218 - KBYTES RECEIVED BY HOST"             },  /* kbytes     */
        { 219, TL_LVL_MONITORING, "RT219 - KBYTES READ BY TAPE DRIVE"           },  /* kbytes     */
        { 220, TL_LVL_MONITORING, "RT220 - KBYTES RECEIVED BY HOST"             },  /* kbytes     */
        { 221, TL_LVL_MONITORING, "RT221 - DATA TRANSFER BANDWIDTH"             },
        { 222, TL_LVL_MONITORING, "RT222 - RECORD(S) TRUNCATED"                 },  /* # recs     */
        { 223, TL_LVL_MONITORING, "RT223 - MULTI VOLUME FILE! COMPRESSION DATA PER FILE SECTION" },
                                                                                    /* kbytes     */
};


tplogger_message_t tplogger_messages_rmcdaemon[] = {

        /* general messages */
        {   0, TL_LVL_ERROR     , "RMC000 - SCSI media changer server not available"}, /* on server ... */
        {   1, TL_LVL_ERROR     , "RMC001 - Robot parameter is mandatory"        },
        {   2, TL_LVL_ERROR     , "RMC002 - Error"                               },  /* ?, ?     */
        {   3, TL_LVL_ERROR     , "RMC003 - Illegal function"                    },  /* function */
        {   4, TL_LVL_ERROR     , "RMC004 - Error getting request"               },  /* netread  */
        {   5, TL_LVL_ERROR     , "RMC005 - Cannot allocate enough memory"       },
        {   6, TL_LVL_ERROR     , "RMC006 - Invalid value"                       },  /* for ...  */
        {   9, TL_LVL_ERROR     , "RMC009 - Fatal configuration error"           },  /* ?, ?     */
        {  46, TL_LVL_ERROR     , "RMC046 - Request too large"                   },  /* max      */
        {  92, TL_LVL_IMPORTANT , "RMC092 - Got request"                         },  /* by, from */
        {  98, TL_LVL_IMPORTANT , "RMC098 - Log request"                         },

        { 101, TL_LVL_EMERGENCY , "RMC101 - General Emergency Message"           },
        { 102, TL_LVL_ALERT     , "RMC102 - General Alert Message"               },
        { 103, TL_LVL_ERROR     , "RMC103 - General Error Message"               },
        { 104, TL_LVL_WARNING   , "RMC104 - General Warning Message"             },
        { 105, TL_LVL_AUTH      , "RMC105 - General Auth Message"                },
        { 106, TL_LVL_SECURITY  , "RMC106 - General Security Message"            },
        { 107, TL_LVL_USAGE     , "RMC107 - General Usage Message"               },
        { 108, TL_LVL_SYSTEM    , "RMC108 - General System Message"              },
        { 109, TL_LVL_IMPORTANT , "RMC109 - General Important Message"           },
        { 110, TL_LVL_MONITORING, "RMC110 - General Monitoring Message"          },
        { 111, TL_LVL_DEBUG     , "RMC111 - General Debug Message"               },

};


/**
 * Return the number of messages for the different facilities.
 *
 * @param self : reference to the tplogger struct
 *
 * @returns    : the number of available messages
 */
int DLL_DECL tplogger_nb_messages( tplogger_t *self ) {

        if( 0 == strcmp( self->tl_name, "taped" ) ) {

                return( ARRAY_ENTRIES( tplogger_messages_tpdaemon ) );
        }

        if( 0 == strcmp( self->tl_name, "rtcpd" ) ) {

                return( ARRAY_ENTRIES( tplogger_messages_rtcpd ) );
        }

        if( 0 == strcmp( self->tl_name, "rmcd" ) ) {

                return( ARRAY_ENTRIES( tplogger_messages_rmcdaemon ) );
        }

        if( 0 == strcmp( self->tl_name, "stdio" ) ) {

                return( ARRAY_ENTRIES( tplogger_messages_tpdaemon ) );
        }

        return 0;
}
