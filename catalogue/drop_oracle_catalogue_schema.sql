WHENEVER SQLERROR EXIT 1
SET SERVEROUTPUT ON
SET FEEDBACK OFF

VARIABLE plSqlReturnValue NUMBER

DECLARE
  SchemaIsLockedException EXCEPTION;
  TYPE NameList IS TABLE OF VARCHAR2(30);

  tables NameList := NameList(
    'CTA_CATALOGUE',
    'ARCHIVE_ROUTE',
    'TAPE_FILE',
    'ARCHIVE_FILE',
    'TAPE',
    'REQUESTER_MOUNT_RULE',
    'REQUESTER_GROUP_MOUNT_RULE',
    'ADMIN_USER',
    'ADMIN_HOST',
    'STORAGE_CLASS',
    'TAPE_POOL',
    'LOGICAL_LIBRARY',
    'MOUNT_POLICY');

  sequences NameList := NameList(
    'ARCHIVE_FILE_ID_SEQ');

  PROCEDURE dropTableIfExists(tableName IN VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ' || tableNAme;
    DBMS_OUTPUT.PUT_LINE('Table ' || tableName || ' dropped');
  EXCEPTION
    WHEN OTHERS THEN
      -- ORA-00942: table or view does not exis
      IF SQLCODE = -942 THEN
        DBMS_OUTPUT.PUT_LINE('Table ' || tableName || ' not dropped because it does not exist');
      ELSE
        RAISE;
      END IF;
  END;

  PROCEDURE dropSequenceIfExists(sequenceName IN VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE ' || sequenceName;
    DBMS_OUTPUT.PUT_LINE('Sequence ' || sequenceName || ' dropped');
  EXCEPTION
    WHEN OTHERS THEN
      -- ORA-02289: sequence does not exist
      IF SQLCODE = -2289 THEN
        DBMS_OUTPUT.PUT_LINE('Sequence ' || sequenceName || ' not dropped because it does not exist');
      ELSE
        RAISE;
      END IF;
  END;

  FUNCTION catalogueSchemaIsLocked RETURN BOOLEAN IS
    TYPE StatusList IS TABLE OF VARCHAR(100);
    schemaLocked INTEGER;
    schemaStatus VARCHAR(100);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT SCHEMA_STATUS FROM CTA_CATALOGUE' INTO schemaStatus;
    RETURN 'LOCKED' = schemaStatus;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Catalogue schema considered LOCKED because CTA_CATALOGUE table is corrupted');
        DBMS_OUTPUT.PUT_LINE('CTA_CATALOGUE table is empty when it should contain one row');
        RETURN TRUE;
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('SQLCODE=' || SQLCODE);
      -- ORA-00942: table or view does not exis
      IF SQLCODE = -942 THEN
        DBMS_OUTPUT.PUT_LINE('Catalogue schema considered UNLOCKED because the CTA_CATALOGUE table does not exist');
        RETURN FALSE;
      -- ORA-01422: exact fetch returns more than requested number of rows
      ELSIF SQLCODE = -1422 THEN
        DBMS_OUTPUT.PUT_LINE('Catalogue schema considered LOCKED because CTA_CATALOGUE table is corrupted');
        DBMS_OUTPUT.PUT_LINE('CTA_CATALOGUE table contains more than one row when it should contain one');
        RETURN TRUE;
      ELSE
        RAISE;
      END IF;
  END;

BEGIN
  -- Non-zero means failure
  :plSqlReturnValue := 1;

  IF catalogueSchemaIsLocked() THEN
    DBMS_OUTPUT.PUT_LINE('Aborting drop of catalogue schema objects: Schema is LOCKED');
  ELSE
    FOR i IN 1 .. tables.COUNT LOOP
      dropTableIfExists(tables(i));
    END LOOP;

    FOR i IN 1 .. sequences.COUNT LOOP
      dropSequenceIfExists(sequences(i));
    END LOOP;

    -- Zero means success
    :plSqlReturnValue := 0;
  END IF;
END;
/

EXIT :plSqlReturnValue
