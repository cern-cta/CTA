/******************************************************************************
 *                      castor/tape/legacymsg/RtcpFileRqst.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_LEGACYMSG_RTCPFILERQST
#define CASTOR_TAPE_LEGACYMSG_RTCPFILERQST

#include "castor/tape/legacymsg/RtcpSegmentAttributes.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor    {
namespace tape      {
namespace legacymsg {

  /**
   * The fields that are common to both an RtcpFileRqstMsgBody and an
   * RtcpFileRqstErrMsgBody.
   */
  struct RtcpFileRqst {
    char filePath[CA_MAXPATHLEN+1];  // Disk file path
    char tapePath[CA_MAXPATHLEN+1];  // Tape device file
    char recfm[CA_MAXRECFMLEN+1];                            
    char fid[CA_MAXFIDLEN+1];        // Tape file ID
    char ifce[5];                    // Network interface name
    char stageId[CA_MAXSTGRIDLEN+1]; // Stage request ID

    uint32_t volReqId;       // VDQM volume request ID
    int32_t  jobId;          // Local RTCOPY server job ID
    int32_t  stageSubReqId;  // Stage subrequest ID (-1 for SHIFT)
    uint32_t umask;          // Client umask
    int32_t  positionMethod; // TPPOSIT_FSEQ, TPPOSIT_FID, TPPOSIT_EOI and TPPOSIT_BLKID
    int32_t  tapeFseq;       // If position_method == TPPOSIT_FSEQ
    int32_t  diskFseq;       // Disk file sequence number. This is the order number of the
                             // current disk file in the request.                    
    int32_t  blockSize;      // Tape blocksize (bytes)
    int32_t  recordLength;   // Tape record length (bytes)
    int32_t  retention;      // File retention time
    int32_t  defAlloc;       // 0 = no deferred allocation

    // Flags
    int32_t  rtcpErrAction; // SKIPBAD or KEEPFILE
    int32_t  tpErrAction;   // NOTRLCHK and IGNOREEOI
    int32_t  convert;       // EBCCONV, FIXVAR
    int32_t  checkFid;      // CHECK_FILE, NEW_FILE
    uint32_t concat;        // CONCAT, CONCAT_TO_EOD, NOCONCAT or NOCONCAT_TO_EOD

    // Intermediate processing status used mainly to checkpoint in case of a
    // retry of a partially successful request.            
    uint32_t procStatus; // RTCP_WAITING, RTC_POSITIONED, RTCP_FINISHED

    // Final return code and timing information                         
    int32_t  cprc;               // Copy return status
    uint32_t tStartPosition;     // Start time for position to this file
    uint32_t tEndPosition;       // End time for position to this file
    uint32_t tStartTransferDisk; // Start time for transfer to/from disk
    uint32_t tEndTransferDisk;   // End time for transfer to/from disk
    uint32_t tStartTransferTape; // Start time for tranfser to/from tape
    uint32_t tEndTransferTape;   // End time for tranfser to/from tape
    unsigned char blockId[4];    // If position_method == TPPOSIT_BLKID


    // 64 bit quantities

    // Compression statistics (actual transfer size)
    uint64_t offset;    // Start offset in disk file
    uint64_t bytesIn;   // Bytes from disk (write) or bytes from tape (read)
    uint64_t bytesOut;  // Bytes to tape (write) or bytes to disk (read)        
    uint64_t hostBytes; // Tape bytes read/written including the labels
    uint64_t nbRecs;    // Number of records copied

    // Imposed size restrictions (input parameters)
    uint64_t maxNbRec;  // Read only "maxnbrec" records
    uint64_t maxSize;   // Copy only "maxsize" bytes
    uint64_t startSize; // Accumulated size

    // CASTOR segment attributes
    RtcpSegmentAttributes segAttr;

    Cuuid_t stgReqId;     // Unique identifier given by the stager
  }; // struct RtcpFileRqst

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_RTCPFILERQST
