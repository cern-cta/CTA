DECLARE
  unused NUMBER;
BEGIN
  -- Check to see if the castor_read user exists
  SELECT user_id INTO unused FROM all_users WHERE username = 'CASTOR_READ';

  -- Grant select on all tables, excluding temporary ones to the castor_read
  -- account. Note: there is no easy GRANT SELECT ON ANY syntax here so we
  -- must loop on all tables.
  FOR a IN (SELECT table_name FROM user_tables WHERE temporary = 'N')
  LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON '||a.table_name||' TO castor_read';
  END LOOP;
  -- The castor_read account is not mandatory so if it doesn't exist do nothing
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
