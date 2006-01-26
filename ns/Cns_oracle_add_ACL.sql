

set timing on;

ALTER TABLE Cns_file_metadata
ADD (guid CHAR(36), csumtype VARCHAR2(2), csumvalue VARCHAR2(32), acl VARCHAR2(3900));

ALTER TABLE Cns_file_metadata
ADD CONSTRAINT file_guid UNIQUE (guid);


CREATE TABLE Cns_symlinks (
       fileid NUMBER,
       linkname VARCHAR2(1023));

ALTER TABLE Cns_symlinks
       ADD CONSTRAINT pk_l_fileid PRIMARY KEY (fileid);

ALTER TABLE Cns_symlinks
       ADD CONSTRAINT fk_l_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);

CREATE INDEX PARENT_FILEID_IDX on Cns_file_metadata(PARENT_FILEID);

DROP TABLE schema_version;
CREATE TABLE schema_version (major NUMBER(1), minor NUMBER(1), patch NUMBER(1));
INSERT INTO schema_version VALUES (1, 1, 1);


quit;

