; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; +                   SELECT BEST FILESYSTEM                   +
; +              PLEASE REPLACE @INPUTxxx@ WITH YOUR DATA      +
; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(clear)
(load* castor.clp)			; Load CASTOR definitions
(defmodule MAIN (import CASTOR ?ALL))	; Import CASTOR definition in MAIN space
(batch* castorweight.clp)		; Load CASTOR weight rules
(bind ?*Debug* 0)			; Debug mode (0=no,1=yes)
;
; IN HERE PLEASE SET THE MODE (0=read,1=write) LIKE THAT:
; (bind ?*Mode* 0)			; Read-from-disk mode
; @INPUTMODE@
;
; IN HERE PLEASE UPLOAD THE DISK SERVERS LIKE THAT:
;(make-instance of DISKSERVER
;	(diskserverName server1)
;	(diskserverStagerName anystager)
;	(diskserverUpdate 123456789)
;	(diskserverStatus AVAILABLE)
;	(diskserverOsname Linux)
;	(diskserverArch Intel)
;	(diskserverTotalMem 501)
;	(diskserverFreeMem 433)
;	(diskserverTotalRam 501)
;	(diskserverFreeRam 6)
;	(diskserverTotalSwap 2047)
;	(diskserverFreeSwap 2020)
;	(diskserverTotalProc 2)
;	(diskserverAvailProc 2)
;	(diskserverLoad 800)
;	(diskserverNfs 6)
;	(filesystemName /shift/server1/data01/
;			/shift/server1/data02/
;			/shift/server1/data03/
;			/shift/server1/data04/
;			/shift/server1/data05/
;			/shift/server1/data06/)
;	(filesystemStatus AVAILABLE AVAILABLE AVAILABLE AVAILABLE AVAILABLE AVAILABLE)
;	(filesystemFactor 1 1 1 1 1 1)
;	(filesystemReadFactor 1 1 1 1 1 1)
;	(filesystemWriteFactor 1 1 1 1 1 1)
;	(filesystemPartition /dev/sdb1 /dev/sdc1 /dev/sdd1 /dev/sde1
;			     /dev/sdf1 /dev/sdg1)
;	(filesystemTotalSpace 73263 73263 73263 73263 73263 73263)
;	(filesystemFreeSpace 0 0 0 0 0 0)
;	(filesystemReadRate 0 0 0 0 0 0)
;	(filesystemWriteRate 0 0 0 0 0 0)
;	(filesystemNbRdonly 0 0 0 0 0 0)
;	(filesystemNbWronly 0 0 0 0 0 0)
;	(filesystemNbReadWrite 0 0 0 0 0 0)
;	(diskserverInterfaceName eth0)
;	(diskserverInterfaceStatus up)
;	(diskserverMemFactor 1)
;)
; @INPUTFS@

; AND THEN EXECUTE THE FOLLOWING:
;(run)                                   ; Run the engine
;(progn$
;   (?fs                          ; on all fileystems
;    (sort filesystemWeight>             ; sorted by weight
;	  (find-all-instances 
;	   ((?filesystem FILESYSTEM)) 
;	   (= 1 ?filesystem:flag)
;	   )
;     )
;   )
;   (bind ?filesystemName (send ?fs get-filesystemName))
;   (printout t ?filesystemName crlf)
;)
;(exit)



