/* Switch on tapegatewayd */
 
BEGIN 
 DELETE FROM castorconfig 
   WHERE class='tape' AND  key='daemonName';
 INSERT INTO castorconfig (class, key, value, description) 
	VALUES ('tape', 'daemonName', 'rtcpclientd','name of the daemon use to interface the tape system');
 COMMIT;
END;
/

/* Recalls */

ALTER TRIGGER tr_Tape_Pending disable;


/* Migrations */

ALTER TRIGGER tr_Stream_Pending disable;


/* clean up db */

DECLARE 
  idsList "numList";
BEGIN
  DELETE FROM TapeGatewaySubRequest RETURNING id BULK COLLECT INTO idsList;  
  FORALL i IN idsList.FIRST ..  idsList.LAST 
	DELETE FROM id2type WHERE  id= idsList(i);
  DELETE FROM TapeGatewayRequest RETURNING id BULK COLLECT INTO idsList;  
  FORALL i IN idsList.FIRST ..  idsList.LAST 
	DELETE FROM id2type WHERE  id= idsList(i);
  COMMIT;
END;
/



	
	
