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

create or replace
TRIGGER TR_Tape_TapeGatewayRequestId
BEFORE INSERT OR UPDATE OF tapeGatewayRequestId ON Tape
FOR EACH ROW
BEGIN
  IF (:new.tapeGatewayRequestId IN (0)) THEN
    :new.tapeGatewayRequestId := NULL;
  END IF;
END;
/

/* We need to add a tapeGatewayRequestId for the tape
   which could be updated by the total backdoor framwork
  in castor::db::ora::OraStagerSvc::createTapeCopySegmentsForRecall
  [...]
          if (recallerPolicyStr.empty())
          tp->setStatus(castor::stager::TAPE_PENDING);
  This bypasses all SQL procedures (this case is covered if there is a policy)
*/
create or replace
TRIGGER TR_Tape_TapeGatewayRequestId
BEFORE INSERT OR UPDATE OF Status ON Tape
FOR EACH ROW
BEGIN
  IF (TapegatewaydIsRunning AND :new.Status IN (1) AND 
	:new.TapegatewayRequestId IS NULL) THEN
    :new.tapeGatewayRequestId := ids_seq.nextval;
  END IF;
END;
/

create or replace
PROCEDURE resurrectSingleTapeForRecall (
  tapeId  IN NUMBER,
)
AS
BEGIN
  IF (TapegatewaydIsRunning) THEN
    UPDATE Tape T
       SET T.TapegatewayRequestId = ids_seq.nextval,
           T.status = 1  -- TAPE_PENDING
     WHERE T.id = tapeId 
       AND T.tpMode = 0; -- TAPE_READ
  ELSE
    UPDATE Tape T
       SET T.status = 1  -- TAPE_PENDING
     WHERE T.id = tapeId 
       AND T.tpMode = 0; -- TAPE_READ
  END IF;
  COMMIT;
END;
/
