/* accessors to ObjStatus table */
CREATE OR REPLACE FUNCTION getObjStatusName(inObject VARCHAR2, inField VARCHAR2, inStatusCode INTEGER)
RETURN VARCHAR2 AS
  varstatusName VARCHAR2(2048);
BEGIN
  SELECT statusName INTO varstatusName
    FROM ObjStatus
   WHERE object = inObject
     AND field = inField
     AND statusCode = inStatusCode;
  RETURN varstatusName;
END;
/

CREATE OR REPLACE PROCEDURE setObjStatusName(inObject VARCHAR2, inField VARCHAR2,
                                             inStatusCode INTEGER, inStatusName VARCHAR2) AS
BEGIN
  INSERT INTO ObjStatus (object, field, statusCode, statusName)
  VALUES (inObject, inField, inStatusCode, inStatusName);
END;
/

