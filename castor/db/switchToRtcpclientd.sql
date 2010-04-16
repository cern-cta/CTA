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
