; WARNING - THIS MODULE IS NOT 64BITS COMPLIANT
; 64BITS QUANTITIES SHOULD BE HANDLED BY A SPECIAL CLASS (TODO)
; Search for (64BITS) in the text below
;
; PROPOSAL FOR 64BITS QUANTITIES: Use the u64subr.h syntax, e.g. with a unit
;
; $Id: castor.clp,v 1.9 2005/04/26 08:19:01 jdurand Exp $
; (c) CASTOR CERN/IT/ADC/CA 2004 - Jean-Damien.Durand@cern.ch
;
; ====================
; CASTOR EXPERT SYSTEM
; ====================
(defmodule CASTOR			; Module name
	(export ?ALL)			; Export everything
)

; ==============
; CASTOR GLOBALS
; ==============
(defglobal
	?*Debug* = 0			
					; Debug the module
	?*Mode* = 0			; Files access modes for module castorweight.clp
					; Mode = 0 is a read-only access (default)
					; Mode = 1 is a write-only access
					; Mode = 2 is a read-write access
	?*MaxStreamSize* = 400          ; Max stream size for migrator
					; and recaller, in MB
	?*MaxNbFiles* = 100		; Maximum number of files to
					; return - for migrator,
					; recaller and garbage
					; collector
	; +++++++++++++++++++++++++++++++++++++++++
	; Default values for importance of metrics
	; when computing the weights
	; +++++++++++++++++++++++++++++++++++++++++
	?*ramSign* = 1                  ; Does increase of ram increase weight?
	?*memSign* = 1                  ; Does increase of mem increase weight?
	?*swapSign* = -1                ; Does increase of swap increase weight?
	?*procSign* = 1                 ; Does increase of proc increase weight?
	?*loadSign* = -1                ; Does increase of load increase weight?
	?*ioSign* = -1                  ; Does increase of i/o increase weight?
	?*streamSign* = -1              ; Does increase of number of stream increase weight?
	?*ramImportance* = 1.           ; How ram counts into a diskserver weight
	?*memImportance* = 1.           ; How mem counts into a diskserver weight
	?*swapImportance* = 1.          ; How swap counts into a diskserver weight
	?*procImportance* = 1.          ; How proc counts into a diskserver weight
	?*loadImportance* = 2.          ; How load counts into a diskserver weight
	?*ioImportance* = 5.            ; How i/o counts into a diskserver weight
	?*streamImportance* = 0.        ; How number of streams counts into a diskserver weight

	;; Please make sure that filesystemReadImportance + filesystemWriteImportance gives 1.

	?*filesystemReadImportance* = 0.5; How readRate counts into a filesystem weight
	?*filesystemWriteImportance* = 0.5; How writeRate counts into a filesystem weight

	;; Please make sure that readImportance + writeImportance + readWriteImportance gives 1.

	?*readImportance* = 1.        ; How a read stream counts into a diskserver weight
	?*writeImportance* = 1.       ; How a read stream counts into a diskserver weight
	?*readWriteImportance* = 1.   ; How a read/write stream counts into a diskserver weight

	;; You might want to make sure than a given fsDeviation is at least equal to...:
	?*minReadWeight* = 0.5          ; Minimum value for a fsDeviation in read-only mode
	?*minWriteWeight* = 0.5         ; Minimum value for a fsDeviation in write-only mode
	?*minReadWriteWeight* = 0.5     ; Minimum value for a fsDeviation in read/write-only mode

	?*minIo* = 0.                   ; Minimum i/o, forced if necessary
)

; ===========================
; CASTOR FILECLASS DEFINITION
; ===========================
(defclass FILECLASS			; Class name
	(is-a USER)			; Inherit default methods
	(role concrete)			; Cannot directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot fileclassId		; ID
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassName		; Name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot fileclassUid		; Uid filter
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassGid		; Gid filter
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassFlags		; Internal flags
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMaxDrives	; Maximum number of drives
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMinFileSize	; Minimum file size (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMaxFileSize	; Maximum file size (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMaxSegSize	; Maximum segment size (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMigrInterval	; Minimum time between migrations
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassMinTime		; Minimum time before being migrated
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassNbCopies		; Number of copies
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot fileclassRetenpOnDisk	; Retention Period on Disk
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot fileclassTppools	; Tape pools
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
)

; ========================
; CASTOR UNIQUE DEFINITION
; ========================
(defclass UNIQUE
	(is-a USER)			; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot uniqueId			; Unique ID (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot uniqueServer		; Server
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
)

; ======================
; CASTOR STAT DEFINITION
; ======================
(defclass STAT
	(is-a USER)			; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot statMode			; File mode
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statUid			; File uid
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statGid			; File gid
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statSize			; File size (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statAtime			; File access time (64BITS after 2038!)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statCtime			; File creation time (64BITS after 2038!)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statMtime			; File modification time (64BITS after 2038!)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot statStatus		; File status
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type SYMBOL)		; ("m" means migrated)
	)
)

; ==========================================================
; CASTOR COPY DEFINITION : COPY NUMBER AS IN THE NAME SERVER
; ==========================================================
(defclass COPY
	(is-a USER)			; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot copyNo			; Copy number
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
)

; =======================================================================================================
; CASTOR DISKCOPY DEFINITION : COPY ON DISK (a copyno in the name server can have several copies on disk)
; =======================================================================================================
(defclass DISKCOPY
	(is-a USER)			; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot diskcopyNo		; Copy number on what is on disk
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskcopyServer	; Corresponding disk server
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot diskcopyFs		; Corresponding file system
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot diskcopyStatus	; Status of the disk file
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type SYMBOL)		; Is a symbol
	)
	(slot diskcopySize		; Size of the disk file (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskcopyAllocSize	; Allocated Size of the disk file (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
)

; ======================
; CASTOR FILE DEFINITION
; ======================
(defclass FILE
	(is-a UNIQUE STAT COPY DISKCOPY); File inherits from them
	(role concrete)			; Can be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot fileclass			; Different files can share the same fileclass
		(type SYMBOL)
		(pattern-match reactive); Changes triggers pattern-matching
		(default ?NONE)
	)
        (slot weight                    ; Associated priority (weight)
                (type FLOAT)            ; I'll give it a try with INTEGER
                (pattern-match reactive); Changes triggers pattern-matching
                (default -1.)
        )
	(slot catalogId			; Unique ID in the catalog
		(type INTEGER)		; Will be a symbol (Cuuid) later
		(pattern-match reactive); Changes triggers pattern-matching
		(default ?NONE)
	)
	(slot fileFlag			; Used to say that the file
					; has been looked up
		(type INTEGER)		; Is an integer
		(pattern-match reactive); Changes triggers
					; pattern-matching
		(create-accessor read-write)
		(default -1)            ; -1 not looked up
		                        ;  0 rejected
		                        ;  1 assigned
	)
)

; =======================================
; CASTOR DISKSERVER MONITORING DEFINITION
; =======================================
(defclass DISKSERVER
	(is-a USER)		; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot diskserverName		; Machine name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot diskserverStagerName	; Corresponding stager name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot diskserverUpdate		; Time of last update (64BITS)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverStatus		; State of the machine
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type SYMBOL)		; Is an integer
	)
	(slot diskserverOsname		; Operating System name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(slot diskserverArch		; Architecture name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	; ------
	; MEMORY
	; ------
	(slot diskserverMemImportance	; Memory importance
		(default ?*memImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverTotalMem	; Available memory (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverFreeMem		; Free memory (MB)
					; (taking into account account buffers and cache)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverMemFactor	; Memory correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	(slot diskserverCorrectedFreeMemMax	; Max corrected Free memory (MB)
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	; ---
	; RAM
	; ---
	(slot diskserverRamImportance	; Ram importance
		(default ?*ramImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverTotalRam	; Available memory (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverFreeRam		; Free memory (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverCorrectedFreeRamMax	; Max corrected Free memory (MB)
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	(slot diskserverRamFactor	; Ram correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	; ----
	; SWAP
	; ----
	(slot diskserverSwapImportance	; Swap importance
		(default ?*swapImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverTotalSwap	; Available swap (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverFreeSwap	; Free swap (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverCorrectedFreeSwapMax	; Max corrected Free swap (MB)
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	(slot diskserverSwapFactor	; Swap correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	; ----
	; PROC
	; ----
	(slot diskserverProcImportance	; Proc importance
		(default ?*procImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverTotalProc	; Total number of processors
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverAvailProc	; Number of available processors
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverCorrectedAvailProcMax	; Max corrected number of available processors (MB)
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	(slot diskserverProcFactor	; Processor correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	; ----
	; LOAD
	; ----
	(slot diskserverLoadImportance	; Load importance
		(default ?*loadImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverLoad		; One minute BSD load average * 100
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverCorrectedLoadMax	; Max corrected load
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	(slot diskserverLoadFactor	; Load correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	; --------------------------------------
	; I/O (computed using FILESYSTEMS below)
	; --------------------------------------
	(slot readImportance	; Read stream importance
		(default ?*readImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot readWeight	; Read stream weight
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot writeImportance	; Write stream importance
		(default ?*writeImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot writeWeight	; Write stream weight
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot readWriteImportance	; Read/Write stream importance
		(default ?*readWriteImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot readWriteWeight	; Read/Write stream weight
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverIoDone	        ; Flag to say that I/O on all
					; diskservers is done
		(default 0)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers
					; pattern-matching
		(storage shared)
		(type INTEGER)		; Is an integer
	)
	(slot diskserverIo	        ; I/o
		(default -1)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(slot diskserverIoImportance	; I/o importance
		(default ?*ioImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverStreamImportance; Number of streams importance
		(default ?*streamImportance*)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(slot diskserverCorrectedIoMax	; Max corrected i/o
		(default -1)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
		(storage shared)
	)
	(slot diskserverIoFactor	; i/o correcting factor
		(default 1.)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	; -----------
	; FILESYSTEMS
	; -----------
	(slot diskserverNfs		; Number of filesystems
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemStatus	; Filesystem status
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type SYMBOL)		; Is a symbol
	)
	(multislot filesystemName		; Filesystem name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(multislot filesystemFactor	; Filesystem factor
		(default 1.)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	(multislot filesystemCorrectedIo; Filesystem global corrected io
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	(slot filesystemCorrectedIoDone; Flag to tell that corrected I/O on fileystem was computed
	        (default 0)
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemPartition	; Corresponding partition
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(multislot filesystemTotalSpace	; Available space (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemFreeSpace	; Free space (MB)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemReadRate	; Current read rate (MB/s)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemReadImportance
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(multislot filesystemReadFactor	; Read Rate correcting factor
		(default 1.)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	(multislot filesystemWriteRate	; Current write rate (MB/s)
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemWriteImportance
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is an integer
	)
	(multislot filesystemWriteFactor	; Write Rate correcting factor
		(default 1.)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type FLOAT)		; Is a float
	)
	(multislot filesystemNbRdonly	; Number of read-only streams
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemNbWronly	; Number of write-only streams
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot filesystemNbReadWrite; Number of read-write streams
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type INTEGER)		; Is an integer
	)
	(multislot diskserverInterfaceName	; Interface name
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type STRING)		; Is a string
	)
	(multislot diskserverInterfaceStatus	; Interface status ("up" or "down")
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
		(type SYMBOL)		; Is an integer
	)
	(multislot filesystemReadWeight	; Read weight
		(type FLOAT)		; Is a float
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
	)
	(multislot filesystemWriteWeight	; Write weight
		(type FLOAT)		; Is a float
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
	)
	(multislot filesystemReadWriteWeight	; Read/Write weight
		(type FLOAT)		; Is a float
		(visibility public)	; Any instance can see that slot
		(pattern-match reactive); Changes triggers pattern-matching
	)
	(slot diskserverFlag		; Used to say that the rule can be fired or not
		(type INTEGER)		; Is an integer
		(pattern-match reactive); Changes triggers pattern-matching
		(default 0)
	)
	(slot diskserverWeight		; diskserver Weight
		(default 0.)
		(type FLOAT)		; Is a float
		(pattern-match reactive); Changes triggers pattern-matching
	)
	(slot diskserverClassFlag	; Variable used to say that other rules can be fired
		(type INTEGER)		; Is an integer
		(pattern-match reactive); Changes triggers pattern-matching
		(storage shared)	; Is common to all instances
		(default 0)
	)
)

; ==================
; SORTED FILESYSTEMS
; ==================
(defclass FILESYSTEM
	(is-a USER)		; Inherit default methods
	(role concrete)			; Cannot be directly created
	(pattern-match reactive)	; Changes triggers pattern-matching
	(slot flag	; Dummy variable used to get everything in a find-all-instances
		(type INTEGER)		; Is an integer
		(pattern-match reactive); Changes triggers pattern-matching
		(storage shared)	; Is common to all instances
		(default 1)
	)
	(slot diskserverInstance	; Machine instance
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
	)
	(slot filesystemIndex		; Filesystem index in this
					; instance adress
		(default ?NONE)		; No default
		(visibility public)	; Any instance can see that slot
		(type INTEGER)		; Is an integer
	)
)

; CLIPS does not directly support object containment, so we do ourself
; the message-handlers
(defmessage-handler FILESYSTEM get-diskserverWeight primary ()
  (send ?self:diskserverInstance get-diskserverWeight)
)
(defmessage-handler FILESYSTEM put-diskserverWeight primary (?weight)
  (send ?self:diskserverInstance put-diskserverWeight ?weight)
)
(defmessage-handler FILESYSTEM add-diskserverWeight primary (?weight)
  (send ?self:diskserverInstance put-diskserverWeight (+ ?weight (send ?self:diskserverInstance get-diskserverWeight)))
)
(defmessage-handler FILESYSTEM get-diskserverName primary ()
  (send ?self:diskserverInstance get-diskserverName)
)
(defmessage-handler FILESYSTEM get-diskserverLoad primary ()
  (send ?self:diskserverInstance get-diskserverLoad)
)
(defmessage-handler FILESYSTEM get-diskserverNfs primary ()
  (send ?self:diskserverInstance get-diskserverNfs)
)
(defmessage-handler FILESYSTEM get-diskserverFreeRam primary ()
  (send ?self:diskserverInstance get-diskserverFreeRam)
)
(defmessage-handler FILESYSTEM get-diskserverFreeSwap primary ()
  (send ?self:diskserverInstance get-diskserverFreeSwap)
)
(defmessage-handler FILESYSTEM get-diskserverFreeMem primary ()
  (send ?self:diskserverInstance get-diskserverFreeMem)
)
(defmessage-handler FILESYSTEM get-diskserverAvailProc primary ()
  (send ?self:diskserverInstance get-diskserverAvailProc)
)
(defmessage-handler FILESYSTEM get-diskserverIo primary ()
  (send ?self:diskserverInstance get-diskserverIo)
)
(defmessage-handler FILESYSTEM get-filesystemReadWeight primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemReadWeight))
)
(defmessage-handler FILESYSTEM get-filesystemWriteWeight primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemWriteWeight))
)
(defmessage-handler FILESYSTEM get-filesystemReadWriteWeight primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemReadWriteWeight))
)
(defmessage-handler FILESYSTEM get-filesystemWeight primary ()
  (switch ?*Mode*
	  (case 0 then
		(abs (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemReadWeight)))
	  )
	  (case 1 then
		(abs (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemWriteWeight)))
	  )
	  (default
	    (abs (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemReadWriteWeight)))
	  )
  )
)
(defmessage-handler FILESYSTEM get-filesystemName primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemName))
)
(defmessage-handler FILESYSTEM get-filesystemFreeSpace primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemFreeSpace))
)
(defmessage-handler FILESYSTEM get-filesystemNbRdonly primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemNbRdonly))
)
(defmessage-handler FILESYSTEM get-filesystemNbWronly primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemNbWronly))
)
(defmessage-handler FILESYSTEM get-filesystemNbReadWrite primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemNbReadWrite))
)
(defmessage-handler FILESYSTEM get-filesystemReadRate primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemReadRate))
)
(defmessage-handler FILESYSTEM get-filesystemWriteRate primary ()
  (nth$ ?self:filesystemIndex (send ?self:diskserverInstance get-filesystemWriteRate))
)
