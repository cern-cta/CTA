/*****************************************************************************
 *              oracleCreate.sql
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
 * @(#)$RCSfile: oracleCreate.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/06 13:18:26 $ $Author: waldron $
 *
 * This script creates a new Castor User Privilege Validator schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement for User Privilege table */
CREATE TABLE USER_PRIVILEGE
       (U_ID NUMBER(6) NOT NULL,
        G_ID NUMBER(6) NOT NULL,
        SRC_HOST VARCHAR2(63) NOT NULL,
        TGT_HOST VARCHAR2(63) NOT NULL,
        PRIV_CAT NUMBER(6) NOT NULL);

ALTER TABLE USER_PRIVILEGE
  ADD CONSTRAINT usr_priv_uk UNIQUE (U_ID, G_ID, SRC_HOST, TGT_HOST, PRIV_CAT);
