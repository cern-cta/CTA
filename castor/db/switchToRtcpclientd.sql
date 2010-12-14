/******************************************************************************
 *                 castor/db/switchToRtcpclientd.sql
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
 * @author Giulia Taurelli, Nicola Bessone and Steven Murray
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

BEGIN
  -- Do nothing and rise an exception if the database is already compatible 
  -- with the rtcpclientd daemon
  IF rtcpclientdIsRunning THEN
    raise_application_error(-20000,
      'PL/SQL switchToRTCPClientd: RTCPClientd already running.');
  END IF;
END;
/

-- The database is about to be modified and is therefore not compatible with
-- either the rtcpclientd daemon or the tape gateway daemon
UPDATE CastorConfig
  SET value = 'SwitchingToRTCPClientd'
  WHERE
    class = 'tape' AND
    key   = 'interfaceDaemon';
COMMIT;

-- Drop the tr_Tape_Pending and tr_Stream_Pending triggers as they are
-- specific to the tape gateway and have no place in an rtcpclientd schema
DROP TRIGGER tr_Tape_Pending;
DROP TRIGGER tr_Stream_Pending;

-- The major reset procedure
-- Tape copies go back to to be migrated and we restart from basically scratch for most of the states.
-- From TAPECOPY_CREATED, leave.
-- From TAPECOPY_TOBEMIGRATED, leave.
-- From TAPECOPY_WAITINSTREAMS, leave.
-- From TAPECOPY_SELECTED, move back to TAPECOPY_TOBEMIGRATED, remove VID, no segments expected.
-- From TAPECOPY_TOBERECALLED, leave.
-- From TAPECOPY_STAGED, leave.
-- From TAPECOPY_FAILED, leave.
-- From TAPECOPY_WAITPOLICY, leave.
-- From TAPECOPY_REC_RETRY, move to TAPECOPY_TOBERECALLED, segment is left as is.
-- From TAPECOPY_MIG_RETRY, move back to TO BE MIGRATED.

-- Streams 
-- From STREAM_PENDING, Leave as is
-- From STREAM_WAITDRIVE, Set to pending
-- From STREMA_WAITMOUNT, set to pending
-- From STREAM_RUNING, set to pending
-- From STREAM_WAITSPACE, Leave as is.
-- From STREAM_CREATED, Leave as is.
-- From STREAM_STOPPED, Leave as is.
-- From STREAM_WAITPOLICY, Leave as is.
-- From STREAM_TOBESENTTOVDQM, we have a busy tape attached and they have to be free.

-- Tapes
-- From TAPE_UNSED, Leave as is.
-- From TAPE_PENDING, Leave as is.
-- From TAPE_WAITDRIVE, reset to PENDING
-- From TAPE_WAITMOUNT, reset to PENDING
-- From TAPE_MOUNTED, reset to PENDING
-- From TAPE_FINISHED, Leave as is. (Assuming it is an end state).
-- From TAPE_FAILED, Leave as is.
-- From TAPE_UNKNOWN, Leave as is. (Assuming it is an end state).
-- From TAPE_WAITPOLICY, Leave as is. (For the rechandler to take).

-- Segments
-- From SEGMENT_UNPROCESSED, Leave as is.
-- From SEGMENT_FILECOPIED, (apparently unused)
-- From SEGMENT_FAILED, (
-- From SEGMENT_SELECTED,
-- From SEGMENT_RETRIED,

DECLARE
  idsList "numList";
BEGIN
  DELETE FROM TapeGatewaySubRequest RETURNING id BULK COLLECT INTO idsList;

  FORALL i IN idsList.FIRST ..  idsList.LAST
    DELETE FROM id2type WHERE  id= idsList(i);

  DELETE FROM TapeGatewayRequest RETURNING id BULK COLLECT INTO idsList;

  FORALL i IN idsList.FIRST ..  idsList.LAST
    DELETE FROM id2type WHERE  id= idsList(i);
END;
/

-- The database is now compatible with the rtcpclientd daemon
UPDATE CastorConfig
  SET value = 'rtcpclientd'
  WHERE
    class = 'tape' AND
    key   = 'interfaceDaemon';
COMMIT;
