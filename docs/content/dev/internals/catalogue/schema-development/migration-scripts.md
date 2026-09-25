# Create the liquibase changelog file

The Changelog file has to be put into the `catalogue/migrations/liquibase/databasetype` directory (where databasetype is oracle or postgres).

Please respect the following naming convention of the Changelog file: `CURRENT_VERSION_NUMBERToNEW_VERSION_NUMBER.sql`. Example: for a schema upgrade from version 1.0 to 1.1, the Changelog file should be named `1.0to1.1.sql`

#### 1. Update the CTA_CATALOGUE table

Update the CTA_CATALOGUE table with the status "UPGRADING" and the NEXT_SCHEMA_VERSION_MAJOR/MINOR numbers. The NEXT_* numbers are the future schema version numbers.

```SQL
--changeset ccaffy:1 failOnError:true dbms:oracle
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:"1.0" SELECT CONCAT(CONCAT(CAST(SCHEMA_VERSION_MAJOR as VARCHAR(10)),'.'), CAST(SCHEMA_VERSION_MINOR AS VARCHAR(10))) AS CATALOGUE_VERSION FROM CTA_CATALOGUE;

UPDATE CTA_CATALOGUE SET STATUS='UPGRADING';
UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MAJOR=1;
UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MINOR=1;

--rollback UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MAJOR=NULL;
--rollback UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MINOR=NULL;
--rollback UPDATE CTA_CATALOGUE SET STATUS='PRODUCTION';
```

!!! danger
    This ChangeSet is very important as it will prevent CTA components from starting during an upgrade of the Catalogue schema

#### 2. Create your own ChangeSets

Each changeset should be preceded by the following statements:

```
--changeset <author_name>:<id> failOnError:true dbms:<databasetype>
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:"1.0" SELECT CONCAT(CONCAT(CAST(SCHEMA_VERSION_MAJOR as VARCHAR(10)),'.'), CAST(SCHEMA_VERSION_MINOR AS VARCHAR(10))) AS CATALOGUE_VERSION FROM CTA_CATALOGUE;
```

- Replace the &lt;author_name&gt; with your cern username
- Replace the &lt;id&gt; with the number of the changeset (from 1 to N)
- Replace &lt;databasetype&gt; by either oracle or postgresql or mariadb. This will prevent the ChangeSet to be executed if the database to migrate file.

The *precondition-sql-check* is useful to check that the Catalogue schema version is the one expected before executing the update.

#### 3. Update the CTA_CATALOGUE table with the new version of the schema

```SQL
--changeset ccaffy:12 failOnError:true dbms:oracle
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:"1.0" SELECT CONCAT(CONCAT(CAST(SCHEMA_VERSION_MAJOR as VARCHAR(10)),'.'), CAST(SCHEMA_VERSION_MINOR AS VARCHAR(10))) AS CATALOGUE_VERSION FROM CTA_CATALOGUE;
UPDATE CTA_CATALOGUE SET SCHEMA_VERSION_MINOR=1;
--rollback UPDATE CTA_CATALOGUE SET SCHEMA_VERSION_MINOR=0
```

#### 4. Update the CTA_CATALOGUE table with the status 'PRODUCTION'

```SQL
--changeset ccaffy:13 failOnError:true dbms:oracle
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:"1.1" SELECT CONCAT(CONCAT(CAST(SCHEMA_VERSION_MAJOR as VARCHAR(10)),'.'), CAST(SCHEMA_VERSION_MINOR AS VARCHAR(10))) AS CATALOGUE_VERSION FROM CTA_CATALOGUE;
UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MAJOR=NULL;
UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MINOR=NULL;
UPDATE CTA_CATALOGUE SET STATUS='PRODUCTION';

--rollback UPDATE CTA_CATALOGUE SET STATUS='UPGRADING';
--rollback UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MAJOR=1;
--rollback UPDATE CTA_CATALOGUE SET NEXT_SCHEMA_VERSION_MINOR=1;
```

!!!tip
    You help yourself by having a look at what is already done in the `CTA/catalogue/migrations/liquibase/oracle` directory.
