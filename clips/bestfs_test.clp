; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; +                   SELECT BEST FILESYSTEM                   +
; +              PLEASE REPLACE @INPUTxxx@ WITH YOUR DATA      +
; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(clear)
(load* castor.clp)			; Load CASTOR definitions
(defmodule MAIN (import CASTOR ?ALL))	; Import CASTOR definition in MAIN space
(batch* castorweight.clp)		; Load CASTOR weight rules
(bind ?*Debug* 1)			; Debug mode (0=no,1=yes)
;
; IN HERE PLEASE SET THE MODE (0=read,1=write) LIKE THAT:
(bind ?*Mode* 0)			; Read-from-disk mode
; @INPUTMODE@
;
; IN HERE PLEASE UPLOAD THE DISK SERVERS LIKE THAT:
(make-instance of DISKSERVER
  (diskserverName lxfsrk5701)
  (diskserverStagerName localhost)
  (diskserverUpdate 1105700765)
  (diskserverStatus Idle)
  (diskserverOsname Linux)
  (diskserverArch i686)
  (diskserverTotalMem 2107809792)
  (diskserverFreeMem 2016960512)
  (diskserverTotalRam 2107809792)
  (diskserverFreeRam 11145216)
  (diskserverTotalSwap 2146754560)
  (diskserverFreeSwap 2146529280)
  (diskserverTotalProc 1)
  (diskserverAvailProc 1)
  (diskserverLoad 10)
  (diskserverNfs 1)
  (filesystemName /shift/lxfsrk5706/data01/)
  (filesystemStatus AVAILABLE)
  (filesystemPartition /dev/sdb1)
  (filesystemTotalSpace 190623)
  (filesystemFreeSpace 1402)

  (filesystemReadRate   0)
  (filesystemWriteRate  0)

  (filesystemNbRdonly    0)
  (filesystemNbWronly    0)

  (filesystemNbReadWrite 0)
  (diskserverInterfaceName eth0)
  (diskserverInterfaceStatus up)
)

(make-instance of DISKSERVER
  (diskserverName lxfsrm505)
  (diskserverStagerName localhost)
  (diskserverUpdate 1105700765)
  (diskserverStatus Idle)
  (diskserverOsname Linux)
  (diskserverArch i686)
  (diskserverTotalMem 1049821184)
  (diskserverFreeMem 941731840)
  (diskserverTotalRam 1049821184)
  (diskserverFreeRam 13660160)
  (diskserverTotalSwap 2146754560)
  (diskserverFreeSwap 2146754560)
  (diskserverTotalProc 2)
  (diskserverAvailProc 2)
  (diskserverLoad 2)
  (diskserverNfs 1)
  (filesystemName /shift/lxfsrm505/data01/)
  (filesystemStatus AVAILABLE)
  (filesystemPartition /dev/sdb1)
  (filesystemTotalSpace 190623)
  (filesystemFreeSpace 174237)

  (filesystemReadRate   20)
  (filesystemWriteRate  15)

  (filesystemNbRdonly    1)
  (filesystemNbWronly    1)

  (filesystemNbReadWrite 0)
  (diskserverInterfaceName eth0)
  (diskserverInterfaceStatus up)
)

; AND THEN EXECUTE THE FOLLOWING:
(run)
(progn$
  (?fs
    (sort filesystemWeight_lt
	  (find-all-instances
	   ((?filesystem FILESYSTEM))
	   (= 1 ?filesystem:flag)
	   )
     )
   )
   (bind ?filesystemName (send ?fs get-filesystemName))
   (printout t "==> "         (send ?fs get-filesystemName)
               " FreeSpace: " (send ?fs get-filesystemFreeSpace)
               " on "         (send ?fs get-diskserverName)
               " Weight "     (send ?fs get-diskserverWeight)
               " fsDeviation " (send ?fs get-filesystemWeight)
               " (freeRam,freeMem,freeSwap,availProc,Load[*100],Io[MB/s])=("
                                (send ?fs get-diskserverFreeRam) ","
                                (send ?fs get-diskserverFreeMem) ","
                                (send ?fs get-diskserverFreeSwap) ","
                                (send ?fs get-diskserverAvailProc) ","
                                (send ?fs get-diskserverLoad) ","
                                (send ?fs get-diskserverIo) ")"
   crlf)
)
(exit)
