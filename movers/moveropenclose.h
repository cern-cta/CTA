/******************************************************************************
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
 * a single function definition for closing a file by any CASTOR mover
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define MOVERHANDLERPORT 15511

/**
 * move_open_file asks for opening a CASTOR file from non-forked movers like xrootd.
 * In this case, allocated transfer slots may expire before the client is ready to
 * transfer the data, regardless from the fact that the mover daemon runs forever.
 * It connects to the diskmanagerd daemon and synchronously wait for the response.
 *
 * port           the port to which to connect
 * transferUuid   the UUID of the transfer to be served, aka the subRequest id
 *
 * The return value is 0 for success and SETIMEDOUT if the slot had timed out meanwhile.
 *
int mover_open_file(const int port, const char* transferUuid);
*/

/**
 * mover_close_file allows a mover to close a CASTOR file after a transfer.
 * It connects to the diskmanagerd daemon and synchronously wait for the response.
 *
 * port           the port to which to connect
 * transferUuid   the UUID of the current transfer, aka the subRequest id
 * filesize       the size of the file (relevant only on Put requests)
 * cksumtype      the type of checksum (relevant only on Put requests)
 * cksumvalue     the hexadecimal value of the checksum (relevant only on Put requests)
 * errorcode      an error code to be passed in case of failure
 * errormsg       an error message
 *
 * errorcode and errormsg are then populated with the result of the operation:
 * if errorcode == 0 the operation was successful, otherwise errormsg contains
 * a useful string to be sent to the client or logged by the mover.
 * The return value is equal to errorcode.
 */

int mover_close_file(const int port, const char* transferUuid, const long long filesize,
                     const char* cksumtype, const char* cksumvalue,
                     int* errorcode, char** errormsg);

#ifdef __cplusplus
}
#endif

