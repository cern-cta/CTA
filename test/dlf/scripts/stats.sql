/*                                  ORACLE                                  */
/*                                                                          */
/* This file contains the code that can be used to test the logical behind  */
/* the generate of the statistics in dlf_jobstats and dlf_reqstats          */
/*                                                                          */
/* These tests are not automated!!!                                         */
/*   - the tests should be run manually through an sql client and then the  */
/*     statistics should be verified against the values proposed in this    */
/*     file, discrepancies indicate that the logic is not working correctly */

/* WARNING: THIS TEST SHOULD NEVER BE PERFORMED ON A PRODUCTION DATABASE    */
/*          IF YOU WISH TO RETAIN THE DATA!                                 */

/* starting request phases */
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDDHH24MISS';


/* hostnames and nshostnames */
INSERT INTO dlf_host_map   VALUES (1, 'lxplus001');
INSERT INTO dlf_nshost_map VALUES (1, 'cns.cern.ch');

/* message number */
INSERT INTO dlf_msg_texts VALUES(4, 8,  'Request starting');
INSERT INTO dlf_msg_texts VALUES(5, 12, 'Job starting');
INSERT INTO dlf_msg_texts VALUES(5, 15, 'Job exiting');

/** Test 1 - Expected Results:
 *
 *  dlf_jobstats
 *      35	200	200	200	1	1	0	300
 *      36	200	200	200	1	1	0	300
 *      37	200	200	200	1	1	0	300
 *      40	200	200	200	1	1	0	300
 *
 *  dlf_reqstats
 *      35	100	100	100	0	1	300
 *      36	100	100	100	0	1	300
 *      37	100	100	100	0	1	300
 *      40	100	100	100	0	1	300
 *
 */

/* starting request phase */
INSERT INTO dlf_messages VALUES (1, SYSDATE - 3/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (1, SYSDATE - 3/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (2, SYSDATE - 3/1440, 00002, '44993320-0000-1000-ac12-ef56b5c5000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (2, SYSDATE - 3/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (3, SYSDATE - 3/1440, 00003, '44993320-0000-1000-ad84-d44f3a1d000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (3, SYSDATE - 3/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (4, SYSDATE - 3/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (4, SYSDATE - 3/1440, 'Type', 40);

/* starting job phase */
INSERT INTO dlf_messages VALUES (5, SYSDATE - 2/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (5, SYSDATE - 2/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (6, SYSDATE - 2/1440, 00002, '44993320-0000-1000-ac12-ef56b5c5000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (6, SYSDATE - 2/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (7, SYSDATE - 2/1440, 00003, '44993320-0000-1000-ad84-d44f3a1d000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (7, SYSDATE - 2/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (8, SYSDATE - 2/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (8, SYSDATE - 2/1440, 'Type', 40);

/* exiting phase */
INSERT INTO dlf_messages VALUES (9,  SYSDATE, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (10, SYSDATE, 00002, '44993320-0000-1000-ac12-ef56b5c5000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (11, SYSDATE, 00003, '44993320-0000-1000-ad84-d44f3a1d000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (12, SYSDATE, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 15, 1111, 1, 1, 0);

COMMIT;

/* reset */
TRUNCATE TABLE dlf_messages;
TRUNCATE TABLE dlf_num_param_values;

COMMIT;

/** Test 2 - Expected Results:
 *
 *  dlf_jobstats
 *      35	100	100	100	0	1	0	300
 *      36	200	300	250	0	2	50	300
 *      37	400	600	500	0	3	81.6	300
 *      40	700	1000	850	0	4	111.8	300
 *
 *  dlf_reqstats
 *      35	0	0	0	0	0	300
 *      36	0	0	0	0	0	300
 *      37	0	0	0	0	0	300
 *      40	0	0	0	0	0	300
 *
 */

/* starting request phase */
INSERT INTO dlf_messages VALUES (1, SYSDATE - 11/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (1, SYSDATE - 11/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (2, SYSDATE - 12/1440, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (2, SYSDATE - 12/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (3, SYSDATE - 13/1440, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (3, SYSDATE - 13/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (4, SYSDATE - 14/1440, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (4, SYSDATE - 14/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (5, SYSDATE - 15/1440, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (5, SYSDATE - 15/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (6, SYSDATE - 16/1440, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (6, SYSDATE - 16/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (7, SYSDATE - 17/1440, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (7, SYSDATE - 17/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (8, SYSDATE - 18/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (8, SYSDATE - 18/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (9, SYSDATE - 19/1440, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (9, SYSDATE - 19/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (10, SYSDATE - 20/1440, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (10, SYSDATE - 20/1440, 'Type', 40);

/* starting job phase */
INSERT INTO dlf_messages VALUES (11, SYSDATE - 1/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (11, SYSDATE - 1/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (12, SYSDATE - 2/1440, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (12, SYSDATE - 2/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (13, SYSDATE - 3/1440, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (13, SYSDATE - 3/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (14, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (14, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (15, SYSDATE - 5/1440, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (15, SYSDATE - 5/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (16, SYSDATE - 6/1440, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (16, SYSDATE - 6/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (17, SYSDATE - 7/1440, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (17, SYSDATE - 7/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (18, SYSDATE - 8/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (18, SYSDATE - 8/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (19, SYSDATE - 9/1440, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (19, SYSDATE - 9/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (20, SYSDATE - 10/1440, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (20, SYSDATE - 10/1440, 'Type', 40);

/* exiting phase */
INSERT INTO dlf_messages VALUES (21, SYSDATE, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (22, SYSDATE, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (23, SYSDATE, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (24, SYSDATE, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (25, SYSDATE, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (26, SYSDATE, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (27, SYSDATE, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (28, SYSDATE, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (29, SYSDATE, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (30, SYSDATE, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 5, 9, 15, 1111, 1, 1, 0);

COMMIT;

/* reset */
TRUNCATE TABLE dlf_messages;
TRUNCATE TABLE dlf_num_param_values;

COMMIT;

/** Test 3 - Expected Results:
 *
 *  dlf_jobstats
 *      35	200	200	200	1	1	0	300
 *      36	200	200	200	2	2	0	300
 *      37	200	200	200	3	3	0	300
 *      40	200	200	200	4	4	0	300
 *
 *  dlf_reqstats
 *      35	100	100	100	1	1	300
 *      36	100	100	100	2	1	300
 *      37	100	100	100	3	1	300
 *      40	100	100	100	4	1	300
 *
 */

/* starting request phase */
INSERT INTO dlf_messages VALUES (1, SYSDATE - 4/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (1, SYSDATE - 4/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (2, SYSDATE - 4/1440, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (2, SYSDATE - 4/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (3, SYSDATE - 4/1440, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (3, SYSDATE - 4/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (4, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (4, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (5, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (5, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (6, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (6, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (7, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (7, SYSDATE - 4/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (8, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (8, SYSDATE - 4/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (9, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (9, SYSDATE - 4/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (10, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 4, 9, 8, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (10, SYSDATE - 4/1440, 'Type', 40);

/* starting job phase */
INSERT INTO dlf_messages VALUES (11, SYSDATE - 3/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (11, SYSDATE - 3/1440, 'Type', 35);
INSERT INTO dlf_messages VALUES (12, SYSDATE - 4/1440, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (12, SYSDATE - 4/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (13, SYSDATE - 3/1440, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (13, SYSDATE - 3/1440, 'Type', 36);
INSERT INTO dlf_messages VALUES (14, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (14, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (15, SYSDATE - 4/1440, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (15, SYSDATE - 4/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (16, SYSDATE - 3/1440, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (16, SYSDATE - 3/1440, 'Type', 37);
INSERT INTO dlf_messages VALUES (17, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (17, SYSDATE - 4/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (18, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (18, SYSDATE - 4/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (19, SYSDATE - 3/1440, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (19, SYSDATE - 3/1440, 'Type', 40);
INSERT INTO dlf_messages VALUES (20, SYSDATE - 4/1440, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 5, 9, 12, 1111, 1, 1, 0);
INSERT INTO dlf_num_param_values VALUES (20, SYSDATE - 4/1440, 'Type', 40);

/* exiting phase */
INSERT INTO dlf_messages VALUES (21, SYSDATE - 2/1440, 00001, '44987639-0000-1000-9faf-ad392699000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (22, SYSDATE - 3/1440, 00002, '44993320-0000-1000-ac12-ef56b5c6000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (23, SYSDATE - 1/1440, 00002, '44993320-0000-1000-ac12-ef56b5c7000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (24, SYSDATE - 2/1440, 00003, '44993320-0000-1000-ad84-d44f3a18000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (25, SYSDATE - 3/1440, 00003, '44993320-0000-1000-ad84-d44f3a19000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (26, SYSDATE - 1/1440, 00003, '44993320-0000-1000-ad84-d44f3a10000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (27, SYSDATE - 2/1440, 00004, '44993320-0000-1000-ac78-dae70481000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (28, SYSDATE - 3/1440, 00004, '44993320-0000-1000-ac78-dae70482000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (29, SYSDATE - 1/1440, 00004, '44993320-0000-1000-ac78-dae70483000', 1, 5, 9, 15, 1111, 1, 1, 0);
INSERT INTO dlf_messages VALUES (30, SYSDATE - 2/1440, 00004, '44993320-0000-1000-ac78-dae70484000', 1, 5, 9, 15, 1111, 1, 1, 0);

/* reset */
TRUNCATE TABLE dlf_messages;
TRUNCATE TABLE dlf_num_param_values;

COMMIT;


/** End-of-File **/
