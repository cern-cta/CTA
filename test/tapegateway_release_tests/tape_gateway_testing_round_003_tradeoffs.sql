/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

-- Fixes added after migration to trunk and integration with UML code generator.

-- Workaround for generator inserting 0s instead of NULLs.

create or replace
TRIGGER TR_TapeCopy_fileTransactionId
BEFORE INSERT OR UPDATE OF fileTransactionId ON TapeCopy
FOR EACH ROW
BEGIN
  IF (:new.fileTransactionId IN (0)) THEN
    :new.fileTransactionId := NULL;
  END IF;
END;
/

