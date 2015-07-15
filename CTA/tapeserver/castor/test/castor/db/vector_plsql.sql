/* SQL and PLSQL definition for tests of calling PL/SQL with vector input */

CREATE TABLE OCCI_VECTOR_RESULTS (timeTag DATE, n NUMBER, t VARCHAR2(1000), idx NUMBER);

CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

CREATE OR REPLACE TYPE "textList" IS TABLE OF VARCHAR2(1000);
/

CREATE OR REPLACE PACKAGE OCCI_VECTOR AS
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index BY binary_integer;
  TYPE "cnumList" IS TABLE OF NUMBER index BY binary_integer;
  
  PROCEDURE OCCI_VECTOR(nums "numList", texts "textList");
END OCCI_VECTOR;
/

CREATE OR REPLACE
PACKAGE BODY OCCI_VECTOR AS
  PROCEDURE OCCI_VECTOR(nums "numList", texts "textList") AS
    i NUMBER;
  BEGIN
    FOR i IN 1..nums.count LOOP
      INSERT INTO OCCI_VECTOR_RESULTS (timeTag, n, t)
      VALUES (CURRENT_TIMESTAMP, nums(i), texts(i), i);
    END LOOP;
    COMMIT;
  END;

END OCCI_VECTOR;
/