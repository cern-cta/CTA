/******************************************************************************
 *              cns_2.1.7-3_to_2.1.7-4.sql
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
 * @(#)$RCSfile: cns_2.1.7-3_to_2.1.7-4.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/04/21 12:16:43 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-3 CNS database to 2.1.7-4
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus and sql developer */
WHENEVER SQLERROR EXIT FAILURE;

/* #35498: Support for path components having 255 characters */
ALTER TABLE Cns_file_metadata MODIFY name VARCHAR2(255);
