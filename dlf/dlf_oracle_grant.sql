------------------------------------------
-- This is dedicated to Lemon Monitoring
------------------------------------------

-- Give access to tables
GRANT SELECT ON castor_dlf.dlf_host_map TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_ns_host_map TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_messages TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_num_param_values TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_str_param_values TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_rq_ids_map TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_facilities TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_severities TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_msg_texts TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_tape_ids TO castordlf_read;


-- This view contains the RH logs that lemon want to see
DROP MATERIALIZED VIEW dlf_LemonRHLogs;
CREATE MATERIALIZED VIEW dlf_LemonRHLogs REFRESH ON COMMIT AS
  SELECT m.time, m.time_usec, m.req_id, p.value
    FROM dlf_messages m, dlf_num_param_values p
   WHERE m.msg_seq_no = p.msg_seq_no
     AND p.par_name = 'Type'
     AND p.value <= 40
     AND p.value >= 35    
     AND m.facility = 4
     AND m.msg_no = 8;

-- This table will contain a summary of the request procesing
-- for each request. Lemon queries will be based on it
DROP TABLE dlf_LemonLogs;
CREATE TABLE dlf_LemonLogs
 (reqId CHAR(36) PRIMARY KEY,
  timeRH DATE, timeRH_usec NUMBER,
  timeJobStart DATE, timeJobStart_usec NUMBER,
  timeJobEnd DATE, timeJobEnd_usec NUMBER,
  hostId NUMBER, type NUMBER);

-- This trigger creates a row in dlf_LemonLogs whenever
-- the dlf_LemonRHLogs view is populated.
-- Since the view may be rebuilt and we don't want double
-- logs for a given query, we catch th DUP_VAL_ON_INDEX error
CREATE OR REPLACE TRIGGER tr_LemonRHMessage
AFTER INSERT ON dlf_LemonRHLogs FOR EACH ROW
BEGIN
  INSERT INTO dlf_LemonLogs
  VALUES (:new.req_id, :new.time, :new.time_usec,
          null, null, null, null, null, :new.value);
EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
  -- nothing, just avoid storing twice the value
  null;
END;

-- This trigger updates the log summary with info
-- from the job starting phase
CREATE OR REPLACE TRIGGER tr_LemonJobMessage1
AFTER INSERT ON dlf_messages FOR EACH ROW
WHEN (new.facility = 5 AND new.msg_no = 12)
BEGIN
  UPDATE dlf_LemonLogs
     SET timeJobStart=:new.time,
         timeJobStart_usec=:new.time_usec,
         hostId=:new.host_id
   WHERE reqId = :new.req_id;
END;

-- This trigger updates the log summary with info
-- from the job exiting phase
CREATE OR REPLACE TRIGGER tr_LemonJobMessage2
AFTER INSERT ON dlf_messages FOR EACH ROW
WHEN (new.facility = 5 AND new.msg_no = 15)
BEGIN
  UPDATE dlf_LemonLogs
     SET timeJobEnd=:new.time,
         timeJobEnd_usec=:new.time_usec
   WHERE reqId = :new.req_id;
END;
