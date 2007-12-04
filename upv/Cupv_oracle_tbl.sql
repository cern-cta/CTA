/*******************************************************************
 *
 * @(#)$RCSfile: Cupv_oracle_tbl.sql,v $ $Revision: 1.1 $ $Date: 2007/12/04 12:28:37 $ $Author: waldron $
 *
 * This file creates the UPV database schema.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for User Privilege table */
CREATE TABLE USER_PRIVILEGE
       (U_ID NUMBER(6) NOT NULL,
        G_ID NUMBER(6) NOT NULL,
        SRC_HOST VARCHAR2(63) NOT NULL,
        TGT_HOST VARCHAR2(63) NOT NULL,
        PRIV_CAT NUMBER(6) NOT NULL);

ALTER TABLE USER_PRIVILEGE
  ADD CONSTRAINT usr_priv_uk UNIQUE (U_ID, G_ID, SRC_HOST, TGT_HOST, PRIV_CAT);
