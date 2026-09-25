# Naming conventions

Please respect the following naming conventions when modifying the CTA Catalogue schema.

!!! warning
    Some versions of Oracle do not allow element names longer than **30** characters.

## PRIMARY KEY constraints

PRIMARY KEY constraints name should always finish with **_PK**

Example:

```SQL
CREATE TABLE Person(
  ID INT NOT NULL,
  NAME VARCHAR(20)
  CONSTRAINT ID_PK PRIMARY KEY(id)
);
```

## FOREIGN KEY constraints

FOREIGN KEY constraints name should always finish with **_FK**

Example:

```SQL
CREATE TABLE TAPE(
  LOGICAL_LIBRARY_ID NUMERIC(20, 0),
  CONSTRAINT TAPE_LOGICAL_LIBRARY_FK FOREIGN KEY(LOGICAL_LIBRARY_ID) REFERENCES LOGICAL_LIBRARY(LOGICAL_LIBRARY_ID)
);
```

## UNIQUE constraints

UNIQUE constraints name should always finish with **_UN**

Example:

```SQL
CREATE TABLE STORAGE_CLASS(
  DISK_INSTANCE_NAME VARCHAR2(100),
  STORAGE_CLASS_NAME VARCHAR2(100),
  CONSTRAINT STORAGE_CLASS_DIN_SCN_UN UNIQUE(DISK_INSTANCE_NAME, STORAGE_CLASS_NAME)
);
```  

## NOT NULL constraints

NOT NULL constraints name should always finish with **_NN**

Example:

```SQL
CREATE TABLE TAPE(
  VID VARCHAR2(100) CONSTRAINT TAPE_V_NN NOT NULL
);
```

## CHECK constraints

CHECK constraints name should always finish with **_CK**

Example:

```SQL
CREATE TABLE TAPE_POOL(
  IS_ENCRYPTED CHAR(1),
  CONSTRAINT TAPE_POOL_IS_ENCRYPTED_BOOL_CK CHECK(IS_ENCRYPTED IN ('0', '1'))
);
```

## INDEX names

INDEX names should always finish with **_IDX**

Example:

```SQL
CREATE INDEX TAPE_TAPE_POOL_ID_IDX ON TAPE(TAPE_POOL_ID);
```

## SEQUENCE names

SEQUENCE names should always finish with **_SEQ**

Example:

```SQL
CREATE SEQUENCE ARCHIVE_FILE_ID_SEQ
  INCREMENT BY 1
  START WITH 4294967296
  NOMAXVALUE
  MINVALUE 1
  NOCYCLE
  CACHE 20
  NOORDER;
```
