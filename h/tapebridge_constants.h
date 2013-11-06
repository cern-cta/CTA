/******************************************************************************
 *                 h/tapebridge_constants.h
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

#ifndef H_TAPEBRIGE_CONSTANTS_H
#define H_TAPEBRIGE_CONSTANTS_H 1

#include <stdint.h>

/**
 * Tape-bridge legacy-message constants.
 */
#define TAPEBRIDGE_BASE_REQTYPE          (0x5100)
#define TAPEBRIDGE_REQ_MIN               (TAPEBRIDGE_BASE_REQTYPE)
#define TAPEBRIDGE_CLIENTINFO_DEPRECATED (TAPEBRIDGE_BASE_REQTYPE+0x01)
#define TAPEBRIDGE_FLUSHEDTOTAPE         (TAPEBRIDGE_BASE_REQTYPE+0x02)
#define TAPEBRIDGE_CLIENTINFO2           (TAPEBRIDGE_BASE_REQTYPE+0x03)
#define TAPEBRIDGE_REQ_MAX               (TAPEBRIDGE_BASE_REQTYPE+0x04)
#define TAPEBRIDGE_VALID_REQTYPE(X) ((X)>TAPEBRIDGE_REQ_MIN && (X)<TAPEBRIDGE_REQ_MAX)

/**
 * The buffer size used when marshalling a tape-bridge legacy-message.
 */
#define TAPEBRIDGE_MSGBUFSIZ (1024)

/**
 * Tape flush mode: TAPEBRIDGE_N_FLUSHES_PER_FILE
 *
 * If the AUL tape format is used then data will be flushed to tape three
 * times per file, once after the header of the file, once again after the
 * end of the data of the file and finally once more after the trailer of
 * the file.
 *
 * If the NL format is used then data will be flushed to tape after the
 * end of the data of each file.
 */
#define TAPEBRIDGE_N_FLUSHES_PER_FILE ((uint32_t)0)

/**
 * Tape flush mode: TAPEBRIDGE_ONE_FLUSH_PER_N_FILES
 *
 * If the AUL tape format is used then data will be flushed to tape after
 * the AUL trailer of the Nth file, where the Nth file is the one that
 * reaches either the maximum number of bytes allowed before a flush or
 * the maximum number of files before a flush.
 *
 * If the NL format is used then data will be flushed to tape after the
 * end of the data of the Nth file.
 */
#define TAPEBRIDGE_ONE_FLUSH_PER_N_FILES ((uint32_t)1)

#endif /* H_TAPEBRIGE_CONSTANTS_H */
