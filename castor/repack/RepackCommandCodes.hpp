/******************************************************************************
 *                      RepackCommandCodes.hpp
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
 * This is the header file to include the representations for the commands for
 * the Repack system. They are used in the RepackClient to trigger an action in
 * the RepackServer.
 *
 * @author Felix Ehm
 *****************************************************************************/


#ifndef REPACKCOMMANDCODES
#define REPACKCOMMANDCODES 1 


namespace castor{
  namespace repack {
    enum RepackServerCodes {
        REPACK              = 2,
        RESTART             = 3,
        REMOVE_TAPE         = 4,
        GET_STATUS          = 6,
        GET_STATUS_ALL      = 8,
        GET_STATUS_ARCHIVED = 10,   // TODO
        RESTART_REPACK      = 12,   // TODO
        ARCHIVE             = 14,
        GET_STATUS_RUNNING  = 16   // TODO?
        };
  }
}
#endif
