/* This file contains SQL code that fills the database */
/* with some test data allowing us to debug CASTOR     */

/* Declare a FileClass for the Alice Data Challenge */
DECLARE 
  newid NUMBER;
BEGIN
  INSERT INTO FileClass (name, minfilesize , maxfilesize, nbcopies , id) 
  VALUES ('alicemdc6', 0, 0, 1, ids_seq.nextval) 
  RETURNING id INTO newid;
  INSERT INTO id2type (id, type) VALUES (newid, 10);
END;
