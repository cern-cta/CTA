/******************************************************************************
 *                h/tapeBridgeFlushedToTapeMsgBody.h
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

#ifndef H_TAPEBRIDGEFLUSHEDTOTAPEMSGBODY_H
#define H_TAPEBRIDGEFLUSHEDTOTAPEMSGBODY_H 1

#include "h/Castor_limits.h"
#include "h/osdep.h"

#include <stdint.h>

/**
 * The body of a TAPEBRIDGE_FLUSHEDTOTAPE message.
 */
typedef struct {
  /**
   * The Volume request ID associated with the tape-mount.
   */
  uint32_t volReqId;

  /**
   * The tape-file sequence-number of the last file successfully flushed to
   * tape.
   *
   * The value of this field must be greater than 0.
   */
  uint32_t tapeFseq;

  /**
   * The total number of bytes actually written to tape in order to record
   * every file within the batch of files for which this flush was done.
   */
  uint64_t batchBytesToTape;
} tapeBridgeFlushedToTapeMsgBody_t;

#define TAPEBRIDGEFLUSHEDTOTAPEMSGBODY_SIZE ( \
  LONGSIZE + /* volReqID                  */  \
  LONGSIZE + /* tapeFseq                  */  \
  HYPERSIZE  /* bytesWrittenToTapeByFlush */)

#endif /* H_TAPEBRIDGEFLUSHEDTOTAPEMSGBODY_H */
