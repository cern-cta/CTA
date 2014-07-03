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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/******************************************************************************
 * Please note that this file is part of the internal API of the rmc daemon
 * and its client software and should therefore not be distributed to end users
 *****************************************************************************/

#pragma once

/**
 * Returns the index of the specified character within the specified string.
 * The search for the character is performed from left to right and stops at the
 * first coccurence of the character.
 *
 * @param str The string to be searched.
 * @param c The character to be seacrhed for.
 * @return The index of the character if it was found else -1 if not.
 */
int rmc_find_char(const char *const str, const char c);

