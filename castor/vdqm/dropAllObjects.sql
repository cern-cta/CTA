/*
select 'drop ' || object_type || ' ' || object_name || ';' as command
from user_procedures where object_type='FUNCTION' order by object_name;
select 'drop ' || object_type || ' ' || object_name || ';' from user_procedures where object_type='PROCEDURE' order by object_name;
select 'drop ' || object_type || ' ' || object_name || ';' from user_procedures where object_type='PACKAGE' order by object_name;
select 'drop view ' || view_name || ';' from user_views order by view_name;
select 'drop sequence ' || SEQUENCE_NAME || ';' from user_sequences order by sequence_name;
select 'drop table ' || table_name || ';' from user_tables order by table_name;
*/

drop FUNCTION GETTIME;

drop PROCEDURE ALLOCATEDRIVE;
drop PROCEDURE REQUESTTOSUBMIT;
drop PROCEDURE REUSETAPEALLOCATION;

drop PACKAGE CASTORVDQM;

drop view CANDIDATEDRIVEALLOCATIONS_VIEW;
drop view TAPEDRIVESHOWQUEUES_VIEW;
drop view TAPEREQUESTSSHOWQUEUES_VIEW;

drop sequence IDS_SEQ;

drop table CLIENTIDENTIFICATION;
drop table DEVICEGROUPNAME;
drop table ERRORHISTORY;
drop table ID2TYPE;
drop table TAPEACCESSSPECIFICATION;
drop table TAPEDRIVE2TAPEDRIVECOMP;
drop table TAPEDRIVECOMPATIBILITY;
drop table TAPEDRIVEDEDICATION;
drop table TAPEREQUEST;
drop table TAPESERVER;
drop table VDQMTAPE;
-- Drop TAPEDRIVE last because it is referenced by foreign keys
drop table TAPEDRIVE;

