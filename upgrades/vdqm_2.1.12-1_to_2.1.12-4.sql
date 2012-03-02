/******************************************************************************
 *                 vdqm_2.1.12-1_to_2.1.12-4.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * This script upgrades a CASTOR v2.1.12-1 VDQM database to v2.1.12-4
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_8_3'
     AND release = '2_1_12_4'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'VDQM'
     AND release LIKE '2_1_12_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the VDQM before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_8_3', '2_1_12_4', 'TRANSPARENT');
COMMIT;


/* Update and revalidation of PL-SQL code */
/******************************************/
CREATE OR REPLACE PACKAGE castorVdqm AS

  /**
   * This procedure tries to allocate a free tape drive to a pending tape
   * request.
   *
   * @param returnVar has a value of 1 if a free drive was successfully
   * allocated to a pending request, 0 if no possible allocation could be found
   * or -1 if an allocation was found but was invalidated by other threads
   * before the appropriate locks could be taken.
   * @param tapeDriveIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the ID of the allocated tape drive, else
   * the value of this parameter will be undefined.
   * @param tapeDriveIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the name of the allocated tape drive, else
   * the value of this parameter will be undefined.
   * @param tapeRequestIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the ID of the pending request, else the
   * value of this parameter will be undefined.
   * @param tapeRequestVidVar if a free drive was successfully allocated then
   * the value of this parameter will be the VID of the pending request, else
   * the value of this parameter will be undefined.
   */
  PROCEDURE allocateDrive(returnVar OUT NUMBER, tapeDriveIdVar OUT NUMBER,
    tapeDriveNameVar  OUT NOCOPY VARCHAR2, tapeRequestIdVar  OUT NUMBER,
    tapeRequestVidVar OUT NOCOPY VARCHAR2);

  /**
   * This procedure gets a new matched request to be submitted to rtcpd.
   *
   * @outTapeReqId the ID of the matched request.
   */
  PROCEDURE getRequestToSubmit(outTapeReqId OUT NUMBER);

  /**
   * This procedure tries to reuse a drive allocation.
   *
   * @param tapeIdVar the ID of the tape of the current drive allocation.
   * @param tapeDriveIdVar the ID of the drive of the current drive allocation.
   * @param returnVar has a value of 1 if the specified drive allocation was
   * successfully reused, 0 if no possible reuse was found or -1 if a possible
   * reuse was found but was invalidated by other threads before the appropriate
   * locks could be taken.
   * @param tapeRequestIdVar if the drive allocation was successfully reused
   * then the value of this parameter will be the ID of the newly assigned
   * request, else the value of this parameter will be undefined.
   */
  PROCEDURE reuseDriveAllocation(tapeIdVar IN NUMBER, tapeDriveIdVar IN NUMBER,
    accessModeVar IN NUMBER, returnVar OUT NUMBER, tapeRequestIdVar OUT NUMBER);

  /**
   * This procedure inserts the specified drive dedications into the database.
   *
   * @param driveNameVar the name of the tape drive to be dedicated
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param dedicateVar the dedication string
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   * @exception castorVdqmException.drive_not_found_application if the
   * specified tape drive cannot be found in the database.
   * @exception castorVdqmException.drive_server_not_found if the specified
   * tape drive and tape server combination cannot be found in the database.
   * @exception castorVdqmException.drive_dgn_not_found if the specified tape
   * drive and DGN combination cannot be found in the database.
   */
  PROCEDURE dedicateDrive(driveNameVar IN VARCHAR2, serverNameVar IN VARCHAR2,
    dgNameVar IN VARCHAR2, dedicateVar IN VARCHAR2);

  /**
   * This procedure inserts the specified tape dedication into the database.
   * If a tape drive is not specified, i.e. driveNAmeVar, serverNAmeVar and
   * dgNameVar are set to NULL, then all dedications for the specified tape
   * will be removed from the database.
   *
   * @param driveNameVar the name of the tape drive
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param vidVar the VID of the tape to be dedicated
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   * @exception castorVdqmException.drive_not_found_application if the
   * specified tape drive cannot be found in the database.
   * @exception castorVdqmException.drive_server_not_found if the specified
   * tape drive and tape server combination cannot be found in the database.
   * @exception castorVdqmException.drive_dgn_not_found if the specified tape
   * drive and DGN combination cannot be found in the database.
   */
  PROCEDURE dedicateTape(vidVar IN VARCHAR2, driveNameVar IN VARCHAR2,
    serverNameVar IN VARCHAR2, dgNameVar IN VARCHAR2);

  /**
   * This procedure deletes the specified drive from the database.
   *
   * @param driveNameVar the name of the tape drive to be delete
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param resultVar is 0 if the deletion was successful. -1 if the specified
   * tape server does not exist, -2 if the specified device group name does not
   * exist, -3 if the specified tape drive does not exist and -4 if the
   * specified drive has a job assigned.
   */
  PROCEDURE deleteDrive(driveNameVar IN VARCHAR2, serverNameVar IN VARCHAR2,
    dgNameVar IN VARCHAR2, resultVar OUT INTEGER);

  /**
   * This procedure tries to move a the state of the specified request from
   * REQUEST_BEINGSUBMITTED to REQUEST_SUBMITTED.
   *
   * This state transition may not be possible if the corresponding drive and
   * tape request states have been modified by other threads.  For example a
   * thread handling a tape drive request message may have put the drive into
   * the down state.
   *
   * If the state of the request cannot be moved to REQUEST_SUBMITTED, then
   * this procedure will do nothing.
   *
   * All the details of the actions taken by this procedure are reflected in
   * the xxxxBeforeVar and xxxxAfterVar OUT parameters of this procedure.
   *
   * Note that except for database ids, a xxxxBefore or a xxxxAfter
   * parameter will contain the value -1 if the actual value is unknown.
   * In the case of database ids, a value of NULL may mean unknown or
   * NULL.
   *
   * @param driveIdVar the ID of the drive
   * @param requestIdVar the ID of the tape request
   * @param returnVar has a value of 1 if the state of the request was
   * successfully moved to REQUEST_SUBMITTED, else 0
   * @param driveExistsVar -1 = unknown, 0 = no drive, 1 = drive exists
   * @param driveStatusBeforeVar -1 = unknown, else drive status before this
   * procedure
   * @param driveStatusAfterVar -1 = unknown, else drive status after this
   * procedure
   * @param runningRequestIdBeforeVar the id of the drive's running request
   * before this procedure
   * @param runningRequestIdAfterVar the id of the drive's running request
   * after this procedure
   * @param requestExistsVar -1 = unknown, 0 = no request, 1 request exists
   * @param requestStatusBeforeVar -1 = unknown, else the request status before
   * this procedure
   * @param requestStatusAfterVar -1 = unknown, else the request status after
   * this procedure
   * @param requestDriveIdBeforeVar the id of the request's drive before this
   * procedure
   * @param requestDriveIdAfterVar the id of the request's drive after this
   * procedure
   */
  PROCEDURE requestSubmitted(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    returnVar                 OUT NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER);

  /**
   * Resets the specified drive and request.
   *
   * The status of the drive is set to REQUEST_PENDING and its running request
   * is set to NULL.
   *
   * The status of the request is set to REQUEST_PENDING and its drive is set
   * to NULL.
   *
   * All the details of the actions taken by this procedure are reflected in
   * the xxxxBeforeVar and xxxxAfterVar OUT parameters of this procedure.
   *
   * Note that except for database ids, a xxxxBefore or a xxxxAfter
   * parameter will contain the value -1 if the actual value is unknown.
   * In the case of database ids, a value of NULL may mean unknown or
   * NULL.
   *
   * @param tapeDriveIdVar the ID of the drive
   * @param tapeRequestIdVar the ID of the tape request
   * @param driveExistsVar -1 = unknown, 0 = no drive, 1 = drive exists
   * @param driveStatusBeforeVar -1 = unknown, else drive status before this
   * procedure
   * @param driveStatusAfterVar -1 = unknown, else drive status after this
   * procedure
   * @param runningRequestIdBeforeVar the id of the drive's running request
   * before this procedure
   * @param runningRequestIdAfterVar the id of the drive's running request
   * after this procedure
   * @param requestExistsVar -1 = unknown, 0 = no request, 1 request exists
   * @param requestStatusBeforeVar -1 = unknown, else the request status before
   * this procedure
   * @param requestStatusAfterVar -1 = unknown, else the request status after
   * this procedure
   * @param requestDriveIdBeforeVar the id of the request's drive before this
   * procedure
   * @param requestDriveIdAfterVar the id of the request's drive after this
   * procedure
   */
  PROCEDURE resetDriveAndRequest(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER);

  /**
   * This procedure sets the priority of a volume.
   *
   * @param priority the priority where 0 is the lowest priority and INT_MAX is
   * the highest.
   * @param clientUIDVar the user id of the client.
   * @param clientGIDVar the group id fo the client.
   * @param clientHostVar the host of the client.
   * @param vidVar the visual identifier of the volume.
   * @param tpModeVar the tape access mode.  Valid values are either 0 meaning
   * write-disabled or 1 meaning write-enabled.
   * @param lifespanTypeVar the type of lifespan to be assigned to the priority
   * setting.  Valid values are either 0 meaning single-shot or 1 meaning
   * unlimited.
   */
  PROCEDURE setVolPriority(priorityVar IN NUMBER, clientUIDVar IN NUMBER,
    clientGIDVar IN NUMBER, clientHostVar IN VARCHAR2, vidVar IN VARCHAR2,
    tpModeVar IN NUMBER, lifespanTypeVar IN NUMBER);

  /**
   * This procedure deletes the specified volume priority if it exist, else
   * does nothing.
   *
   * @param vidVar the visual identifier of the volume.
   * @param tpModeVar the tape access mode.  Valid values are either 0 meaning
   * write-disabled or 1 meaning write-enabled.
   * @param lifespanTypeVar the type of lifespan to be assigned to the priority
   * setting.  Valid values are either 0 meaning single-shot or 1 meaning
   * unlimited.
   * @param returnVar the ID id of the volume priority row if one was deleted,
   * else 0.
   * @param priorityVar the priority of the deleted volume priority if one was
   * deleted, else undefined.
   * @param clientUIDVar the client UID of the deleted volume priority if one
   * was deleted, else undefined.
   * @param clientGIDVar the client GID of the deleted volume priority if one
   * was deleted, else undefined.
   * @param clientHostVar the client host of the deleted volume priority if one
   * was deleted, else undefined.
   */
  PROCEDURE deleteVolPriority(vidVar IN VARCHAR2, tpModeVar IN NUMBER,
    lifespanTypeVar IN NUMBER, returnVar OUT NUMBER, priorityVar OUT NUMBER,
    clientUIDVar OUT NUMBER, clientGIDVar OUT NUMBER,
    clientHostVar OUT NOCOPY VARCHAR2);

  /**
   * This procedure deletes old single-mount volume priorities.
   *
   * @param maxAgeVar the maximum age of a volume priority in seconds.
   * @param prioritiesDeletedVar the number of volume priorities deleted.
   */
  PROCEDURE deleteOldVolPriorities(maxAgeVar IN NUMBER,
    prioritiesDeletedVar OUT NUMBER);

END castorVdqm;
/

/**
 * See the castorVdqm package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqm AS

  /**
   * Determines whether or not the specified drive and GID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the GID of the client.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesGidDriveDedications(
    driveIdVar IN NUMBER,
    gidVar     IN NUMBER)
    RETURN NUMBER AS
    nbGidDedicationsVar NUMBER;
  BEGIN
    -- Count the number of GID dedications for the drive
    -- (there should only be one)
    SELECT COUNT(*) INTO nbGidDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND egid IS NOT NULL;

    -- Drive passes if there are no GID dedications for it
    IF nbGidDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more GID dedications

    -- Count the number of matching GID dedications
    SELECT COUNT(*) INTO nbGidDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND gidVar = TapeDriveDedication.egid;

    -- As there are GID dedications for the drive, it only passes if at least
    -- one matches
    IF nbGidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesGidDriveDedications;


  /**
   * Determines whether or not the specified drive and access mode pass the
   * dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param accessModeVar the access mode of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesModeDriveDedication(
    driveIdVar    IN NUMBER,
    accessModeVar IN NUMBER)
    RETURN NUMBER AS
    nbModeDedicationsVar NUMBER;
  BEGIN
    -- Count the number of mode dedications for the drive
    -- (there should only be one)
    SELECT COUNT(*) INTO nbModeDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND accessMode IS NOT NULL;

    -- Drive passes if there are no access mode dedications for it
    IF nbModeDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has a mode dedication

    -- Count the number of matching vid dedications
    -- (there should be a maximum of one)
    SELECT COUNT(*) INTO nbModeDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND TapeDriveDedication.accessMode = accessModeVar;

    -- As there is a mode dedication for the drive, the drive only passes if it
    -- matches
    IF nbModeDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesModeDriveDedication;


  /**
   * Determines whether or not the specified drive and host pass the
   * dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param clientHostVar the client host of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesHostDriveDedications(
    driveIdVar    IN NUMBER,
    clientHostVar IN VARCHAR2)
    RETURN NUMBER AS
    nbHostDedicationsVar NUMBER;
  BEGIN
    -- Count the number of host dedications for the drive
    SELECT COUNT(*) INTO nbHostDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND clientHost IS NOT NULL;

    -- Drive passes if there are no host dedications for it
    IF nbHostDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more host dedications

    -- Count the number of matching host dedications
    SELECT COUNT(*) INTO nbHostDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND REGEXP_LIKE(clientHostVar, TapeDriveDedication.clientHost);

    -- As there are host dedications for the drive, it only passes if at least
    -- one matches
    IF nbHostDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesHostDriveDedications;


  /**
   * Determines whether or not the specified drive and UID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param uidVar     the UID of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesUidDriveDedications(
    driveIdVar IN NUMBER,
    uidVar     IN NUMBER)
    RETURN NUMBER AS
    nbUidDedicationsVar NUMBER;
  BEGIN
    -- Count the number of UID dedications for the drive
    -- (there should only be one)
    SELECT COUNT(*) INTO nbUidDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND euid IS NOT NULL;

    -- Drive passes if there are no UID dedications for it
    IF nbUidDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more UID dedications

    -- Count the number of matching UID dedications
    SELECT COUNT(*) INTO nbUidDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND uidVar = TapeDriveDedication.euid;

    -- As there are UID dedications for the drive, it only passes if at least
    -- one matches
    IF nbUidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesUidDriveDedications;


  /**
   * Determines whether or not the specified drive and VID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesVidDriveDedications(
    driveIdVar IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbVidDedicationsVar NUMBER;
  BEGIN
    -- Count the number of vid dedications for the drive
    SELECT COUNT(*) INTO nbVidDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND vid IS NOT NULL;

    -- Drive passes if there are no vid dedications for it
    IF nbVidDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more vid dedications

    -- Count the number of matching vid dedications
    SELECT COUNT(*) INTO nbVidDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND REGEXP_LIKE(vidVar, TapeDriveDedication.vid);

    -- As there are vid dedications for the drive, it only passes if at least
    -- one matches
    IF nbVidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesVidDriveDedications;


  /**
   * Determines whether or not the specified drive and VID pass the
   * tape-to-drive dedications.
   *
   * @param driveIdVar the ID of the drive.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesTape2DriveDedications(
    driveIdVar IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbDedicationsVar NUMBER;
  BEGIN
    -- Count the number of dedications for the tape
    SELECT COUNT(*) INTO nbDedicationsVar
      FROM Tape2DriveDedication
      WHERE Tape2DriveDedication.vid = vidVar;

    -- Dedications passed if there are no dedications for the tape
    IF nbDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Tape has one or more dedications

    -- Count the number of matching dedications
    SELECT COUNT(*) INTO nbDedicationsVar
      FROM Tape2DriveDedication
      WHERE
        Tape2DriveDedication.vid = vidVar
        AND Tape2DriveDedication.tapeDrive = driveIdVar;

    -- As there are tape dedications, they are only passed if at least one
    -- matches
    IF nbDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesTape2DriveDedications;


  /**
   * This function determines if the specified drive and usage pass all drive
   * and tape dedications.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the gid of the client.
   * @param hostVar    the host of the client.
   * @param modeVar    the tape access mode.
   * @param uidVar     the uid of the client.
   * @param vidVar     the vid of the volume request.
   * @return 1 if all dedications are passed, else 0.
   */
  FUNCTION passesDedications(
    driveIdVar IN NUMBER,
    gidVar     IN NUMBER,
    hostVar    IN VARCHAR2,
    modeVar    IN NUMBER,
    uidVar     IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbVidDedicationsVar NUMBER;
  BEGIN
    IF passesGidDriveDedications(driveIdVar, gidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesHostDriveDedications(driveIdVar, hostVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesModeDriveDedication(driveIdVar, modeVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesUidDriveDedications(driveIdVar, uidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesVidDriveDedications(driveIdVar, vidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesTape2DriveDedications(driveIdVar, vidVar) = 0 THEN
      RETURN 0;
    END IF;

    -- Drive has passed all of its dedications
    RETURN 1;
  END passesDedications;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE allocateDrive(
    returnVar         OUT NUMBER,
    tapeDriveIdVar    OUT NUMBER,
    tapeDriveNameVar  OUT NOCOPY VARCHAR2,
    tapeRequestIdVar  OUT NUMBER,
    tapeRequestVidVar OUT NOCOPY VARCHAR2
    ) AS

    schedulerLockIdVar   INTEGER;
    tapeDriveStatusVar   NUMBER;
    tapeRequestStatusVar NUMBER;

  BEGIN
    returnVar         := 0; -- No possible allocation could be found
    tapeDriveIdVar    := 0;
    tapeDriveNameVar  := '';
    tapeRequestIdVar  := 0;
    tapeRequestVidVar := '';

    -- Take the scheduler lock which is responsible for serializing the
    -- scheduler algorithm
    SELECT id INTO schedulerLockIdVar
      FROM VdqmLock
      WHERE id = 1
      FOR UPDATE;

    -- For each potential drive allocation, i.e. a drive allocation for which
    -- no dedications have been taken into account
    FOR potentialMount IN (
      SELECT driveId, driveName, tapeRequestId, clientEuid, clientEgid,
        clientMachine, accessMode, vid
      FROM PotentialMounts_VIEW
    ) LOOP
      -- The status of the drives and requests maybe modified by other scheduler
      -- threads.  The status of the drives may be modified by threads handling
      -- drive request messages.  The status of the requests may be modified by
      -- threads handling tape request messages.  Therefore get a lock on the
      -- corresponding drive and request rows and retrieve their statuses to see
      -- if the drive allocation is still valid
      BEGIN
        SELECT TapeDrive.status INTO TapeDriveStatusVar
        FROM TapeDrive
        WHERE TapeDrive.id = potentialMount.driveId
        FOR UPDATE;
        SELECT TapeRequest.status INTO TapeRequestStatusVar
        FROM TapeRequest
        WHERE TapeRequest.id = potentialMount.tapeRequestId
        FOR UPDATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          -- A possible drive allocation was found but was invalidated by other
          -- threads before the appropriate locks could be taken
          returnVar := -1;
          RETURN;
      END;

      -- If the drive allocation is no longer valid, i.e. drive status is not
      -- UNIT_UP or request status is not REQUEST_PENDING
      IF(TapeDriveStatusVar != 0) OR (TapeRequestStatusVar != 0) THEN
        -- A possible drive allocation was found but was invalidated by other
        -- threads before the appropriate locks could be taken
        returnVar := -1;
        RETURN;
      END IF;

      IF passesDedications(
        potentialMount.driveId,
        potentialMount.clientEgid,
        potentialMount.clientMachine,
        potentialMount.accessMode,
        potentialMount.clientEuid,
        potentialMount.vid) = 1 THEN

        -- Set the drive id for logging
        tapeDriveIdVar := potentialMount.driveId;

        -- Set the drive name for logging
        TapeDriveNameVar := potentialMount.driveName;

        -- Set the request id for logging
        tapeRequestIdVar := potentialMount.tapeRequestId;

        -- Set the VID of the pending request for logging
        tapeRequestVidVar := potentialMount.vid;

        -- Allocate the free drive to the pending request
        UPDATE TapeDrive SET
          status           = 1, -- UNIT_STARTING
          jobId            = 0,
          modificationTime = castorVdqmCommon.getTime(),
          runningTapeReq   = potentialMount.tapeRequestId
        WHERE
          id = potentialMount.driveId;
        UPDATE TapeRequest SET
          status           = 1, -- MATCHED
          tapeDrive        = potentialMount.driveId,
          modificationTime = castorVdqmCommon.getTime()
        WHERE
          id = potentialMount.tapeRequestId;

        returnVar := 1; -- A free drive has been allocated to a pending request
        RETURN;

      END IF; -- passesDedications

    END LOOP; -- For each potential drive allocation

  END allocateDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE getRequestToSubmit(outTapeReqId OUT NUMBER) AS
    RowLocked EXCEPTION;
    PRAGMA EXCEPTION_INIT (RowLocked, -54);
    CURSOR varCur IS SELECT id FROM TapeRequest WHERE status = 1;  -- 1=MATCHED
  BEGIN
    outTapeReqId := 0;

    OPEN varCur;
    LOOP
      FETCH varCur INTO outTapeReqId;

      IF varCur%NOTFOUND THEN -- The value of outTapeReqId is now indeterminate
        outTapeReqId := 0;
        EXIT; -- Exit the loop
      END IF;

      BEGIN
        SELECT id INTO outTapeReqId FROM TapeRequest
          WHERE id = outTapeReqId AND STATUS = 1 -- 1=MATCHED
          FOR UPDATE NOWAIT;

        UPDATE TapeRequest SET STATUS = 2 -- 2=BEINGSUBMITTED
          WHERE id = outTapeReqId;

        EXIT; -- Exit the loop
      EXCEPTION
        WHEN RowLocked     THEN outTapeReqId := 0;
        WHEN NO_DATA_FOUND THEN outTapeReqId := 0;
      END;
    END LOOP;
  END getRequestToSubmit;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE reuseDriveAllocation(
    tapeIdVar          IN NUMBER,
    tapeDriveIdVar     IN NUMBER,
    accessModeVar      IN NUMBER,
    returnVar         OUT NUMBER,
    tapeRequestIdVar  OUT NUMBER)
  AS
    tapeDriveStatusCheckVar   NUMBER;
    mountedTapeIdCheckVar     NUMBER;
    tapeRequestStatusCheckVar NUMBER;
    tapeAccessSpecIdCheckVar  NUMBER;
    accessModeCheckVar        NUMBER;
  BEGIN
    returnVar        := 0; -- No possible reuse was found
    tapeRequestIdVar := 0;

    -- For each potential reuse of the current mount
    FOR potentialReuse IN (
      SELECT tapeRequestId, clientEuid, clientEgid, clientMachine, accessMode,
        tapeId, vid, driveId
        FROM PotentialReusableMounts_VIEW
        WHERE
              PotentialReusableMounts_VIEW.driveId    = tapeDriveIdVar
          AND PotentialReusableMounts_VIEW.tapeId     = tapeIdVar
          AND PotentialReusableMounts_VIEW.accessMode = accessModeVar
    ) LOOP

      -- The status of the drives including which tapes may be mounted in them,
      -- may be modified by other threads handling drive request messages.  The
      -- status of the requests may be modified by threads handling tape request
      -- messages.  Therefore get a lock on the corresponding drive and request
      -- rows and retrieve their statuses and mounted tape in the case of the
      -- drive, to see if the reuse of the mount is still valid.
      BEGIN
        SELECT TapeDrive.status, TapeDrive.tape
        INTO tapeDriveStatusCheckVar, mountedTapeIdCheckVar
        FROM TapeDrive
        WHERE TapeDrive.id = potentialReuse.driveId
        FOR UPDATE;
        SELECT TapeRequest.status, TapeRequest.tapeAccessSpecification
        INTO tapeRequestStatusCheckVar, tapeAccessSpecIdCheckVar
        FROM TapeRequest
        WHERE TapeRequest.id = potentialReuse.tapeRequestId
        FOR UPDATE;
        SELECT TapeAccessSpecification.accessMode
        INTO accessModeCheckVar
        FROM TapeAccessSpecification
        WHERE TapeAccessSpecification.id = tapeAccessSpecIdCheckVar
        FOR UPDATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          -- A possible reuse of the mount was found but was invalidated by
          -- other threads before the appropriate locks could be taken
          returnVar        := -1;
          RETURN;
      END;

      -- If the reuse of the mount is no longer valid, i.e. the drive's status
      -- is not VOL_MOUNTED or the correct tape is not mounted or the tape
      -- request's status is not REQUEST_PENDING or the access mode no longer
      -- matches
      IF
        (tapeDriveStatusCheckVar   !=             3) OR
        (mountedTapeIdCheckVar     !=     tapeIdVar) OR
        (tapeRequestStatusCheckVar !=             0) OR
        (accessModeCheckVar        != accessModeVar) THEN
          -- A possible reuse of the mount was found but was invalidated by
          -- other threads before the appropriate locks could be taken
          returnVar        := -1;
          RETURN;
      END IF;

      IF passesDedications(
        potentialReuse.driveId,
        potentialReuse.clientEgid,
        potentialReuse.clientMachine,
        potentialReuse.accessMode,
        potentialReuse.clientEuid,
        potentialReuse.vid) = 1 THEN

        -- Reuse the mount with the pending request
        UPDATE TapeRequest SET
          status           = 1, -- MATCHED
          tapeDrive        = tapeDriveIdVar,
          modificationTime = castorVdqmCommon.getTime()
        WHERE id = potentialReuse.tapeRequestId;
        UPDATE TapeDrive SET
          status           = 1, -- UNIT_STARTING
          jobId            = 0,
          runningTapeReq   = potentialReuse.tapeRequestId,
          modificationTime = castorVdqmCommon.getTime()
        WHERE id = tapeDriveIdVar;

        -- The drive allocation was reused
        tapeRequestIdVar := potentialReuse.tapeRequestId;
        returnVar        := 1;
        RETURN;
      END IF;

    END LOOP; -- For each potential reuse of the current mount

  END reuseDriveAllocation;


  /**
   * This procedure parses the specified dedicate string for gid, host, mode,
   * uid and VID dedications.  This procedure raises an application error with
   * an error number of -20001 if it detects an error in the dedicate string.
   *
   * @param dedicateStrVar the dedicate string to be parsed.
   * @param gidVar the extracted gid if there was one, else NULL.
   * @param hostVar the extracted host if there was one, else NULL.
   * @param modeVar the extracted mode if there was one, else NULL.
   * @param uidVar the extracted uid if there was one, else NULL.
   * @param vidVar the extracted VID if there was one, else NULL
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   */
  PROCEDURE parseDedicateStr(
    dedicateStrVar  IN VARCHAR2,
    gidVar         OUT NUMBER,
    hostVar        OUT NOCOPY VARCHAR2,
    modeVar        OUT NUMBER,
    uidVar         OUT NUMBER,
    vidVar         OUT NOCOPY VARCHAR2
    ) AS
    dedicationStrsVar castorVdqmCommon.Varchar256List;
    nameValueListVar  castorVdqmCommon.Varchar256List;
    TYPE NameValue is RECORD (name VARCHAR2(256), val VARCHAR2(256));
    nameValueVar      NameValue;
    TYPE NameValueList IS TABLE OF NameValue INDEX BY BINARY_INTEGER;
    dedicationsVar    NameValueList;
    indexVar          NUMBER := 1;
    dummyNumVar       NUMBER := NULL;
    numAgeVar         NUMBER := 0; -- Number of     age dedications found so far
    numDateStrVar     NUMBER := 0; -- Number of dateStr dedications found so far
    numGidVar         NUMBER := 0; -- Number of     GID dedications found so far
    numHostVar        NUMBER := 0; -- Number of    host dedications found so far
    numModeVar        NUMBER := 0; -- Number of    mode dedications found so far
    numNameVar        NUMBER := 0; -- Number of    name dedications found so far
    numTimeStrVar     NUMBER := 0; -- Number of timeStr dedications found so far
    numUidVar         NUMBER := 0; -- Number of     UID dedications found so far
    numVidVar         NUMBER := 0; -- Number of     VID dedications found so far
  BEGIN
    gidVar  := NULL;
    hostVar := NULL;
    modeVar := NULL;
    uidVar  := NULL;
    vidVar  := NULL;

    -- Return if the dedicate string is NULL or empty
    IF dedicateStrVar IS NULL OR dedicateStrVar = '' THEN
      RETURN;
    END IF;

    -- Split the dedicate string into the individual dedications
    dedicationStrsVar := castorVdqmCommon.tokenize(dedicateStrVar, ',');

    -- Split each individual dedication into a name value pair
    FOR indexVar IN dedicationStrsVar.FIRST .. dedicationStrsVar.LAST LOOP
      nameValueListVar :=
        castorVdqmCommon.tokenize(dedicationStrsVar(indexVar), '=');

      -- Raise an application error if the dedication is not a name value pair
      IF nameValueListVar.count != 2 THEN
        castorVdqmException.throw(castorVdqmException.invalid_drive_dedicate_cd,
          'Invalid drive dedicate string.  ' ||
          'Dedication is not a valid name value pair ''' ||
          dedicationStrsVar(indexVar) || '''');
      END IF;

      nameValueVar.name := LOWER(LTRIM(RTRIM(nameValueListVar(1))));
      nameValueVar.val  := LTRIM(RTRIM(nameValueListVar(2)));

      dedicationsVar(dedicationsVar.count + 1) := nameValueVar;
    END LOOP;

    -- Extract the specific types of dedication (gid, host, etc.), raising an
    -- application error if there is an invalid dedication
    FOR indexVar IN dedicationsVar.FIRST .. dedicationsVar.LAST LOOP
      CASE
        WHEN dedicationsVar(indexVar).name = 'age' THEN
          IF numAgeVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one age dedication. ''');
          END IF;
          numAgeVar := numAgeVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''age'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'datestr' THEN
          IF numDateStrVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one datestr dedication. ''');
          END IF;
          numDateStrVar := numDateStrVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''datestr'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'gid' THEN
          IF numGidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one gid dedication. ''');
          END IF;
          numGidVar := numGidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              gidVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid gid dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'host' THEN
          IF numhostVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one host dedication.');
          END IF;
          numHostVar := numHostVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              SELECT COUNT(*) INTO dummyNumVar
                FROM DUAL WHERE REGEXP_LIKE(dummy,dedicationsVar(indexVar).val);
              hostVar := dedicationsVar(indexVar).val;
            EXCEPTION
              WHEN OTHERS THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid host dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'mode' THEN
          IF numModeVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one mode dedication.');
          END IF;
          numModeVar := numModeVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              modeVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid mode dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'name' THEN
          IF numNameVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one name dedication.');
          END IF;
          numNameVar := numNameVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''name'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'timestr' THEN
          IF numTimeStrVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one timestr dedication.');
          END IF;
          numTimeStrVar := numTimeStrVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''timestr'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'uid' THEN
          IF numUidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one uid dedication.');
          END IF;
          numUidVar := numUidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              uidVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid uid dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'vid' THEN
          IF numVidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one vid dedication.');
          END IF;
          numVidVar := numVidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              SELECT COUNT(*) INTO dummyNumVar
                FROM DUAL WHERE REGEXP_LIKE(dummy,dedicationsVar(indexVar).val);
              vidVar := dedicationsVar(indexVar).val;
            EXCEPTION
              WHEN OTHERS THEN
                castorVdqmException.throw(
                castorVdqmException.invalid_drive_dedicate_cd,
                'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                || '  Invalid vid dedication ''' || dedicationsVar(indexVar).val
                || ''' ' || SQLERRM);
            END;
          END IF;
        ELSE
          castorVdqmException.throw(
            castorVdqmException.invalid_drive_dedicate_cd,
            'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
            || '  Uknown dedication type ''' || dedicationsVar(indexVar).name
            || '''');
      END CASE;
    END LOOP;

  END parseDedicateStr;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE dedicateDrive(driveNameVar IN VARCHAR2, serverNameVar IN  VARCHAR2,
    dgNameVar IN  VARCHAR2, dedicateVar IN  VARCHAR2) AS
    gidVar               NUMBER;
    hostVar              VARCHAR2(256);
    modeVar              NUMBER;
    uidVar               NUMBER;
    vidVar               VARCHAR2(256);
    driveIdVar           NUMBER;
    TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    dedicationsToDelete  dedicationList_t;
  BEGIN

    -- Get the id of the tape drive
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        INNER JOIN TapeServer ON
          TapeDrive.tapeServer = TapeServer.id
        INNER JOIN DeviceGroupName ON
          TapeDrive.deviceGroupName = DeviceGroupName.id
        WHERE
              TapeDrive.driveName    = driveNameVar
          AND TapeServer.serverName  = serverNameVar
          AND DeviceGroupName.dgName = dgNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Lock the tape drive row
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        WHERE
          TapeDrive.id = driveIdVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Parse the dedication string raising an application error with an error
    -- number of -20001 if there is an error in the dedicate string
    parseDedicateStr(dedicateVar, gidVar, hostVar, modeVar, uidVar, vidVar);

    -- Delete all existing dedications associated with tape drive
    SELECT id BULK COLLECT INTO dedicationsToDelete
      FROM TapeDriveDedication
      WHERE TapeDriveDedication.tapeDrive = driveIdVar;

    IF dedicationsToDelete.COUNT > 0 THEN
      FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
        DELETE FROM TapeDriveDedication
          WHERE TapeDriveDedication.id = dedicationsToDelete(i);
      END LOOP;
    END IF;

    -- Insert new dedications
    IF gidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, egid)
        VALUES(ids_seq.nextval, driveIdVar, gidVar);
    END IF;
    IF hostVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, clientHost)
        VALUES(ids_seq.nextval, driveIdVar, hostVar);
    END IF;
    IF modeVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, accessMode)
        VALUES(ids_seq.nextval, driveIdVar, modeVar);
    END IF;
    IF uidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, euid)
        VALUES(ids_seq.nextval, driveIdVar, uidVar);
    END IF;
    IF vidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, vid)
        VALUES(ids_seq.nextval, driveIdVar, vidVar);
    END IF;

  END dedicateDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE dedicateTape(vidVar IN VARCHAR2, driveNameVar IN VARCHAR2,
    serverNameVar IN  VARCHAR2, dgNameVar IN  VARCHAR2) AS
    driveIdVar           NUMBER;
    TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    dedicationsToDelete  dedicationList_t;
    dedicationIdVar      NUMBER;
  BEGIN
    -- If no tape drive is specified then remove all dedications for the
    -- specified tape and return
    IF driveNameVar IS NULL AND serverNameVar IS NULL AND dgNameVar IS NULL
      THEN
      -- Delete all existing dedications for the tape
      SELECT id BULK COLLECT INTO dedicationsToDelete
        FROM Tape2DriveDedication
        WHERE Tape2DriveDedication.vid = vidVar;
      IF dedicationsToDelete.COUNT > 0 THEN
        FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
          DELETE FROM Tape2DriveDedication
            WHERE Tape2DriveDedication.id = dedicationsToDelete(i);
        END LOOP;
      END IF;

      RETURN;
    END IF;

    -- Get the id of the tape drive
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        INNER JOIN TapeServer ON
          TapeDrive.tapeServer = TapeServer.id
        INNER JOIN DeviceGroupName ON
          TapeDrive.deviceGroupName = DeviceGroupName.id
        WHERE
              TapeDrive.driveName    = driveNameVar
          AND TapeServer.serverName  = serverNameVar
          AND DeviceGroupName.dgName = dgNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Lock the tape drive row
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        WHERE
          TapeDrive.id = driveIdVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Delete all existing dedications for the tape
    SELECT id BULK COLLECT INTO dedicationsToDelete
      FROM Tape2DriveDedication
      WHERE Tape2DriveDedication.vid = vidVar;
    IF dedicationsToDelete.COUNT > 0 THEN
      FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
        DELETE FROM Tape2DriveDedication
          WHERE Tape2DriveDedication.id = dedicationsToDelete(i);
      END LOOP;
    END IF;

    -- Insert new dedication
    INSERT INTO Tape2DriveDedication(id, vid, tapeDrive)
      VALUES(ids_seq.nextval, vidVar, driveIdVar);

  END dedicateTape;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteDrive
  ( driveNameVar  IN  VARCHAR2
  , serverNameVar IN  VARCHAR2
  , dgNameVar     IN  VARCHAR2
  , resultVar     OUT INTEGER
  ) AS
    dgnIdVar          NUMBER;
    tapeServerIdVar   NUMBER;
    driveIdVar        NUMBER;
    runningTapeReqVar NUMBER;
  BEGIN
    resultVar := 0;

    BEGIN
      SELECT id
        INTO tapeServerIdVar
        FROM TapeServer
        WHERE
          serverName = serverNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      resultVar := -1; -- Tape server does not exist
      RETURN;
    END;
  
    BEGIN
      SELECT id INTO dgnIdVar FROM DeviceGroupName WHERE dgName = dgNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      resultVar := -2; -- DGN does not exits
      RETURN;
    END;
  
    BEGIN
      SELECT id, runningTapeReq
        INTO driveIdVar, runningTapeReqVar
        FROM TapeDrive
        WHERE
              deviceGroupName = dgnIdVar
          AND tapeServer = tapeServerIdVar
          AND driveName = driveNameVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      resultVar := -3; -- Tape drive does not exist
      RETURN;
    END;
  
    IF runningTapeReqVar IS NOT NULL THEN
      resultVar := -4; -- Drive has a job assigned
      RETURN;
    END IF;
  
    DELETE FROM TapeDrive2TapeDriveComp WHERE parent    = driveIdVar;
    DELETE FROM TapeDriveDedication     WHERE tapeDrive = driveIdVar;
    DELETE FROM TapeDrive               WHERE id        = driveIdVar;

  END deleteDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE requestSubmitted(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    returnVar                 OUT NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER)
  AS
  BEGIN
    returnVar                 :=    0; -- Nothing has been done
    driveExistsVar            :=   -1; -- Unknown
    driveStatusBeforeVar      :=   -1; -- Unknown
    driveStatusAfterVar       :=   -1; -- Unknown
    runningRequestIdBeforeVar := NULL; -- Unknown
    runningRequestIdAfterVar  := NULL; -- Unknown
    requestExistsVar          :=   -1; -- Unknown
    requestStatusBeforeVar    :=   -1; -- Unknown
    requestStatusAfterVar     :=   -1; -- Unknown
    requestDriveIdBeforeVar   := NULL; -- Unknown
    requestDriveIdAfterVar    := NULL; -- Unknown

    -- Try to get a lock on and the status of the corresponding drive row
    BEGIN
      SELECT TapeDrive.status    , TapeDrive.runningTapeReq
        INTO driveStatusBeforeVar, runningRequestIdBeforeVar
        FROM TapeDrive
        WHERE TapeDrive.id = driveIdVar
        FOR UPDATE;

      driveExistsVar           := 1;                         -- Drive exists
      driveStatusAfterVar      := driveStatusBeforeVar;      -- No change yet
      runningRequestIdAfterVar := runningRequestIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        driveExistsVar := 0; -- Drive does not exist
    END;

    -- If the drive no longer exists, then do nothing
    IF driveExistsVar = 0 THEN
      RETURN;
    END IF;

    -- Try to get a lock on and the status of the request row
    BEGIN
      SELECT TapeRequest.status    , TapeRequest.tapeDrive
        INTO requestStatusBeforeVar, requestDriveIdBeforeVar
        FROM TapeRequest
        WHERE TapeRequest.id = requestIdVar
        FOR UPDATE;

      requestExistsVar       := 1;                       -- Request exists
      requestStatusAfterVar  := requestStatusBeforeVar;  -- No change yet
      requestDriveIdAfterVar := requestDriveIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        requestExistsVar := 0; -- Request does not exist
    END;

    -- If the request no longer exists, then do nothing
    IF requestExistsVar = 0 THEN
      RETURN;
    END IF;

    -- If the state of the request cannot be moved to REQUEST_SUBMITTED, then
    -- do nothing
    IF driveStatusBeforeVar      != 1            OR -- 1=UNIT_STARTING
       requestStatusBeforeVar    != 2            OR -- 2=REQUEST_BEINGSUBMITTED
       runningRequestIdBeforeVar != requestIdVar OR
       requestDriveIdBeforeVar   != driveIdVar
      THEN
      RETURN;
    END IF;

    -- Reaching this points means the transition to REQUEST_SUBMITTED is valid

    -- Set the state of the request to REQUEST_SUBMITTED
    UPDATE TapeRequest
      SET TapeRequest.status = 3 -- REQUEST_SUBMITTED = 3
      WHERE TapeRequest.id = requestIdVar;

    -- Set out parameters
    requestStatusAfterVar := 3; -- REQUEST_SUBMITTED = 3
    returnVar             := 1; -- Succeeded

  END requestSubmitted;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE resetDriveAndRequest(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER)
  AS
  BEGIN
    driveExistsVar            :=   -1; -- Unknown
    driveStatusBeforeVar      :=   -1; -- Unknown
    driveStatusAfterVar       :=   -1; -- Unknown
    runningRequestIdBeforeVar := NULL; -- Unknown
    runningRequestIdAfterVar  := NULL; -- Unknown
    requestExistsVar          :=   -1; -- Unknown
    requestStatusBeforeVar    :=   -1; -- Unknown
    requestStatusAfterVar     :=   -1; -- Unknown
    requestDriveIdBeforeVar   := NULL; -- Unknown
    requestDriveIdAfterVar    := NULL; -- Unknown

    -- Try to get a lock on and the status of the corresponding drive row
    BEGIN
      SELECT TapeDrive.status    , TapeDrive.runningTapeReq
        INTO driveStatusBeforeVar, runningRequestIdBeforeVar
        FROM TapeDrive
        WHERE TapeDrive.id = driveIdVar
        FOR UPDATE;

      driveExistsVar           := 1;                         -- Drive exists
      driveStatusAfterVar      := driveStatusBeforeVar;      -- No change yet
      runningRequestIdAfterVar := runningRequestIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        driveExistsVar := 0; -- Drive row does not exist
    END;

    -- Try to get a lock on and the status of the request row
    BEGIN
      SELECT TapeRequest.status    , TapeRequest.tapeDrive
        INTO requestStatusBeforeVar, RequestDriveIdBeforeVar
        FROM TapeRequest
        WHERE TapeRequest.id = requestIdVar
        FOR UPDATE;

      requestExistsVar       := 1;                       -- Request exists
      requestStatusAfterVar  := requestStatusBeforeVar;  -- No change yet
      requestDriveIdAfterVar := requestDriveIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        requestExistsVar := 0; -- Request does not exist
    END;

    -- If the drive exists
    IF driveExistsVar = 1 THEN
      UPDATE TapeDrive
        SET
          TapeDrive.status         = 7,   -- STATUS_UNKNOWN = 7
          TapeDrive.runningTapeReq = NULL -- No running request
        WHERE TapeDrive.id = driveIdVar;

      driveStatusAfterVar      := 7;    -- STATUS_UNKNOWN = 7
      runningRequestIDAfterVar := NULL; -- No running request
    END IF;

    -- If the request exists
    IF requestExistsVar = 1 THEN
      UPDATE TapeRequest
        SET
          TapeRequest.status    = 0,   -- REQUEST_PENDING = 0
          TapeRequest.tapeDrive = NULL -- No drive
        WHERE TapeRequest.id = requestIdVar;

      requestStatusAfterVar  := 0;    -- REQUEST_PENDING = 0
      requestDriveIdAfterVar := NULL; -- No drive
    END IF;
  END resetDriveAndRequest;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE setVolPriority(
    priorityVar     IN NUMBER,
    clientUIDVar    IN NUMBER,
    clientGIDVar    IN NUMBER,
    clientHostVar   IN VARCHAR2,
    vidVar          IN VARCHAR2,
    tpModeVar       IN NUMBER,
    lifespanTypeVar IN NUMBER)
  AS
    priorityIdVar NUMBER;
  BEGIN
    -- Try to select and get a lock on the row representing the priority if one
    -- already exists
    BEGIN
      SELECT id INTO priorityIdVar FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        priorityIdVar := NULL;
    END;

    -- If a row for the priority already exists
    IF priorityIdVar IS NOT NULL THEN

      -- Update it if it has an equal or lower priority
      UPDATE VolumePriority SET
        VolumePriority.priority   = priorityVar,
        VolumePriority.clientUID  = clientUIDVar,
        VolumePriority.clientGID  = clientGIDVar,
        VolumePriority.clientHost = clientHostVar
      WHERE
            VolumePriority.id = priorityIdVar
        AND VolumePriority.priority <= priorityVar;

    -- Else a row for the priority does not yet exist
    ELSE

      -- Try to create one
      BEGIN
        INSERT INTO VolumePriority(id, priority, clientUID, clientGID,
          clientHost, vid, tpMode, lifespanType)
        VALUES(ids_seq.nextval, priorityVar, clientUIDVar, clientGIDVar,
          clientHostVar, vidVar, tpModeVar, lifespanTypeVar)
        RETURNING id INTO priorityIdVar;
      EXCEPTION
        WHEN OTHERS THEN
          -- If another competing thread created the row first
          IF SQLCODE = -1 THEN -- ORA-00001: unique constraint violated

            -- Ensure priorityIdVar is NULL
            priorityIdVar := NULL;

          -- Else an unforeseen error has occured
          ELSE

            -- Reraise the current exception
            RAISE;

          END IF;
      END;

      -- If a row for the priority was not successfully created then
      -- a competing thread created the row first
      IF priorityIdVar IS NULL THEN
        -- Select and get a lock on the row
        SELECT id INTO priorityIdVar FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;

        -- Update the row if it has an equal or lower priority
        UPDATE VolumePriority SET
          priority   = priorityVar,
          clientUID  = clientUIDVar,
          clientGID  = clientGIDVar,
          clientHost = clientHostVar
        WHERE
              VolumePriority.id = priorityIdVar
          AND VolumePriority.priority <= priorityVar;
      END IF;

    END IF; -- If a row for the priority already exists

  END setVolPriority;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteVolPriority(
    vidVar           IN VARCHAR2,
    tpModeVar        IN NUMBER,
    lifespanTypeVar  IN NUMBER,
    returnVar       OUT NUMBER,
    priorityVar     OUT NUMBER,
    clientUIDVar    OUT NUMBER,
    clientGIDVar    OUT NUMBER,
    clientHostVar   OUT NOCOPY VARCHAR2)
  AS
    priorityIdVar NUMBER;
  BEGIN
    returnVar := 0;

    -- Try to get the ID of the volume priority row
    BEGIN
      SELECT id, priority, clientUID, clientGID, clientHost
        INTO priorityIdVar, priorityVar, clientUIDVar, clientGIDVar,
          clientHostVar
        FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;
    EXCEPTION
      -- Do nothing if the specified volume priority row does not exist
      WHEN NO_DATA_FOUND THEN RETURN;
    END;

    -- Delete the volume priority row
    DELETE FROM VolumePriority WHERE VolumePriority.id = priorityIdVar;

    -- Give the ID of the volume priority row that was deleted
    returnVar := priorityIdVar;
  END deleteVolPriority;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteOldVolPriorities(
    maxAgeVar             IN NUMBER,
    prioritiesDeletedVar OUT NUMBER)
  AS
    nowVar NUMBER;
  BEGIN
    prioritiesDeletedVar := 0;

    nowVar := castorVdqmCommon.getTime();

    DELETE FROM VolumePriority
    WHERE
          VolumePriority.lifespanType = 0 -- Single-mount
      AND (nowVar - VolumePriority.modificationTime) > maxAgeVar;

    prioritiesDeletedVar := SQL%ROWCOUNT;
  END deleteOldVolPriorities;

END castorVdqm;
/

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
    EXCEPTION WHEN OTHERS THEN
      -- ignore, so that we continue compiling the other invalid items
      NULL;
    END;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_12_4';
COMMIT;
