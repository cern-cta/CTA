; ============================================
; PRIORITY FOR /ANY/ FILESYSTEM (default rule)
; ============================================
; --------------------------------------------
; Node weight (each fXxx is between 0. and 1.)
; --------------------------------------------
; fRam     = diskserverFreeRam   * diskserverRamFactor   / Max(diskserverFreeRam * diskserverRamFactor)
; fMem     = diskserverFreeMem   * diskserverMemFactor   / Max(diskserverFreeMem * diskserverMemFactor)
; fSwap    = diskserverFreeSwap  * diskserverSwapFactor  / Max(diskserverFreeSwap* diskserverSwapFactor)
; fProc    = diskserverAvailProc * diskserverProcFactor  / Max(diskserverAvailProc* diskserverProcFactor)
; fLoad    = diskserverLoad      * diskserverLoadFactor  / Max(diskserverLoad     * diskserverLoadFactor)
; fIo      = diskserverIo        * diskserverIoFactor    / Max(diskserverIo       * diskserverIoFactor)
; fDiskserver =       ramSign  * fRam   * diskserverRamImportance  +
;                     memSign  * fMem   * diskserverMemImportance  +
;                     swapSign * fSwap  * diskserverSwapImportance +
;                     procSign * fProc  * diskserverProcImportance +
;                     loadSign * fLoad  * diskserverLoadImportance +
;                     ioSign   * fIo    * diskserverIoImportance   +
;                     1
; where diskserverIo is the sum of all filesystemWeightedIo defining a diskserver
; weighted per filesystem
; The weight per filesystem reflects the fact that a filesystem is
; 'better' than another (example: xfs v.s. ext2 - just a example - hey!)
; ----------------------
; Filesystem weight
; ----------------------
; The Read and Write factors reflects the configuration of the disk
; (RAIDx for example)
; fFs            = fDiskserver * ((filesystemNbTot / diskserverNbTot) + (filesystemIoTot / diskserverIoTot))
;
; Unless you get new monitoring info, we suggest that we you select a filesystem, you update the fFs of all
; filesystems on the same machine by the one that you have just selected, except that for this one, indeed,
; you take the original multiplied by 2, e.g.:
; newfFs = fFs * 2
; for all (fs) {
;   if (fs != selectedFs) fFs += selectedfFs
; }
; selectfFs = newfFs
;

; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute free corrected memory over all diskserver instances
; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedFreeMemMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedFreeMemMax ?diskserverCorrectedFreeMemMax)
				(diskserverFreeMem ?diskserverFreeMem)
				(diskserverMemFactor ?diskserverMemFactor)
			)
	(test (< ?diskserverCorrectedFreeMemMax 0))
=>
        (bind ?CorrectedFreeMemMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedFreeMem (* ?p:diskserverFreeMem
					       ?p:diskserverMemFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedFreeMemMax] "
			    ?p:diskserverName ": correctedFreeMem is " ?thisCorrectedFreeMem crlf)
		)
		(if (> ?thisCorrectedFreeMem ?CorrectedFreeMemMax)
		    then
		  (bind ?CorrectedFreeMemMax ?thisCorrectedFreeMem)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedFreeMemMax] diskserverCorrectedFreeMemMax is " ?CorrectedFreeMemMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedFreeMemMax ?CorrectedFreeMemMax)
)

; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute free corrected swap over all diskserver instances
; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedFreeSwapMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedFreeSwapMax ?diskserverCorrectedFreeSwapMax)
				(diskserverFreeSwap ?diskserverFreeSwap)
				(diskserverSwapFactor ?diskserverSwapFactor)
			)
	(test (< ?diskserverCorrectedFreeSwapMax 0))
=>
        (bind ?CorrectedFreeSwapMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedFreeSwap (* ?p:diskserverFreeSwap
						  ?p:diskserverSwapFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedFreeSwapMax] "
			    ?p:diskserverName ": correctedFreeSwap is " ?thisCorrectedFreeSwap crlf)
		)
		(if (> ?thisCorrectedFreeSwap ?CorrectedFreeSwapMax)
		    then
		  (bind ?CorrectedFreeSwapMax ?thisCorrectedFreeSwap)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedFreeSwapMax] diskserverCorrectedFreeSwapMax is " ?CorrectedFreeSwapMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedFreeSwapMax ?CorrectedFreeSwapMax)
)

; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute free corrected ram over all diskserver instances
; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedFreeRamMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedFreeRamMax ?diskserverCorrectedFreeRamMax)
				(diskserverFreeRam ?diskserverFreeRam)
				(diskserverRamFactor ?diskserverRamFactor)
			)
	(test (< ?diskserverCorrectedFreeRamMax 0))
=>
        (bind ?CorrectedFreeRamMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedFreeRam (* ?p:diskserverFreeRam
						  ?p:diskserverRamFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedFreeRamMax] "
			    ?p:diskserverName ": correctedFreeRam is " ?thisCorrectedFreeRam crlf)
		)
		(if (> ?thisCorrectedFreeRam ?CorrectedFreeRamMax)
		    then
		  (bind ?CorrectedFreeRamMax ?thisCorrectedFreeRam)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedFreeRamMax] diskserverCorrectedFreeRamMax is " ?CorrectedFreeRamMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedFreeRamMax ?CorrectedFreeRamMax)
)

; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute avail corrected proc over all diskserver instances
; ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedAvailProcMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedAvailProcMax ?diskserverCorrectedAvailProcMax)
				(diskserverAvailProc ?diskserverAvailProc)
				(diskserverProcFactor ?diskserverProcFactor)
			)
	(test (< ?diskserverCorrectedAvailProcMax 0))
=>
        (bind ?CorrectedAvailProcMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedAvailProc (* ?p:diskserverAvailProc
						  ?p:diskserverProcFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedAvailProcMax] "
			    ?p:diskserverName ": correctedAvailProc is " ?thisCorrectedAvailProc crlf)
		)
		(if (> ?thisCorrectedAvailProc ?CorrectedAvailProcMax)
		    then
		  (bind ?CorrectedAvailProcMax ?thisCorrectedAvailProc)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedAvailProcMax] diskserverCorrectedAvailProcMax is " ?CorrectedAvailProcMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedAvailProcMax ?CorrectedAvailProcMax)
)

; ++++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute corrected load over all diskserver instances
; ++++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedLoadMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedLoadMax ?diskserverCorrectedLoadMax)
				(diskserverLoad ?diskserverLoad)
				(diskserverLoadFactor ?diskserverLoadFactor)
			)
	(test (< ?diskserverCorrectedLoadMax 0))
=>
        (bind ?CorrectedLoadMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedLoad (* ?p:diskserverLoad
						  ?p:diskserverLoadFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedLoadMax] "
			    ?p:diskserverName ": correctedLoad is " ?thisCorrectedLoad crlf)
		)
		(if (> ?thisCorrectedLoad ?CorrectedLoadMax)
		    then
		  (bind ?CorrectedLoadMax ?thisCorrectedLoad)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedLoadMax] diskserverCorrectedLoadMax is " ?CorrectedLoadMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedLoadMax ?CorrectedLoadMax)
)

; ++++++++++++++++++++++++++++++++++
; Compute i/o for a given diskserver
; ++++++++++++++++++++++++++++++++++
(defrule diskserverIo
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverName ?diskserverName)
				(diskserverIo ?diskserverIo)
				(diskserverNfs ?diskserverNfs)
				(filesystemCorrectedIo $?filesystemCorrectedIo)
				(filesystemFactor $?filesystemFactor)
				(filesystemCorrectedIoDone ?filesystemCorrectedIoDone)
				(filesystemName $?filesystemName)
			)
	(test (< ?diskserverIo 0))
	(test (= ?filesystemCorrectedIoDone 1))
=>
        (bind ?thisIo 0)
	; We need first to compute filesystemCorrectedIo for every filesystem
        (loop-for-count (?index 1 ?diskserverNfs)
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverIo] " ?diskserverName ":"
			    (nth$ ?index ?filesystemName) " io is "
			    (nth$ ?index ?filesystemCorrectedIo) ", filesystemFactor is "
			    (nth$ ?index ?filesystemFactor) ", leaving to "
			    (* (nth$ ?index ?filesystemCorrectedIo) (nth$ ?index ?filesystemFactor)) crlf)
		)
		(bind ?thisIo (+ ?thisIo (* (nth$ ?index ?filesystemCorrectedIo) (nth$ ?index ?filesystemFactor))))
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverIo] " ?diskserverName ": diskserverIo is " ?thisIo crlf)
	)
	(if (< ?thisIo ?*minIo*) then
	  (if (!= 0 ?*Debug*) then
	    (printout t "[diskserverIo] " ?diskserverName ": Warning, diskserverIo < " ?*minIo* ", forcing " ?*minIo* crlf)
	  )
	  (bind ?thisIo ?*minIo*)
	)
	(send ?diskserver put-diskserverIo ?thisIo)
	; We check if this is done for all diskservers
	(bind ?Ntodo 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(if (< ?p:diskserverIo 0) then
			(bind ?Ntodo 1)
			(break)
		)
	)
	(if (= ?Ntodo 0) then
		; We use this instance to fire the rule on all diskservers
	        (if (!= 0 ?*Debug*) then
		  (printout t "[diskserverIo] setting diskserverIoDone" crlf)
		  )
		(send ?diskserver put-diskserverIoDone 1)
	)
)

; +++++++++++++++++++++++++++++++++++++
; Compute corrected i/o for filesystems
; +++++++++++++++++++++++++++++++++++++
(defrule filesystemCorrectedIo
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverName ?diskserverName)
				(diskserverNfs ?diskserverNfs)
				(filesystemReadRate $?filesystemReadRate)
				(filesystemWriteRate $?filesystemWriteRate)
				(filesystemReadFactor $?filesystemReadFactor)
				(filesystemWriteFactor $?filesystemWriteFactor)
				(filesystemReadImportance $?filesystemReadImportance)
				(filesystemWriteImportance $?filesystemWriteImportance)
				(filesystemCorrectedIoDone ?filesystemCorrectedIoDone)
				(filesystemName $?filesystemName)
			)
	(test (= ?filesystemCorrectedIoDone 0))
=>
        (bind ?filesystemCorrectedIo (create$ ))
	; We need first to compute filesystemCorrectedIo for every
	; filesystem
	(bind ?correctedFilesystemReadRate (create$ ))
	(bind ?correctedFilesystemWriteRate (create$ ))
	(bind ?maxCorrectedFilesystemReadRate 0.)
	(bind ?maxCorrectedFilesystemWriteRate 0.)
        (loop-for-count (?index 1 ?diskserverNfs)
		(bind ?thisFilesystemReadRate  (* (nth$ ?index ?filesystemReadRate) (nth$ ?index ?filesystemReadFactor)))
		(bind ?thisFilesystemWriteRate (* (nth$ ?index ?filesystemWriteRate) (nth$ ?index ?filesystemWriteFactor)))
		(if (> ?thisFilesystemReadRate
		       ?maxCorrectedFilesystemReadRate)
		    then
		  (bind ?maxCorrectedFilesystemReadRate
			?thisFilesystemReadRate)
		)
		(if (> ?thisFilesystemWriteRate
		       ?maxCorrectedFilesystemWriteRate)
		    then
		  (bind ?maxCorrectedFilesystemWriteRate
			?thisFilesystemWriteRate)
		)
		(if (!= 0 ?*Debug*) then
		  (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			    (nth$ ?index ?filesystemName) " filesystemReadRate is "
			    (nth$ ?index ?filesystemReadRate) ", filesystemReadFactor is "
			    (nth$ ?index ?filesystemReadFactor) " ==> "
			    ?thisFilesystemReadRate crlf)
		  (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			    (nth$ ?index ?filesystemName) " filesystemWriteRate is "
			    (nth$ ?index ?filesystemWriteRate) ", filesystemWriteFactor is "
			    (nth$ ?index ?filesystemWriteFactor) " ==> "
			    ?thisFilesystemWriteRate crlf)
		)
		(if (= 1 ?index)
		   then
		  (bind ?correctedFilesystemReadRate (create$ ?thisFilesystemReadRate))
		  (bind ?correctedFilesystemWriteRate (create$ ?thisFilesystemWriteRate))
		  else
		  (bind ?correctedFilesystemReadRate (insert$
							      ?correctedFilesystemReadRate
							      (+ 1
								 (length ?correctedFilesystemReadRate))
							      ?thisFilesystemReadRate)
		  )
		  (bind ?correctedFilesystemWriteRate (insert$
							      ?correctedFilesystemWriteRate
							      (+ 1
								 (length ?correctedFilesystemWriteRate))
							      ?thisFilesystemWriteRate)
		  )
		 )
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[filesystemCorrectedIo] " ?diskserverName ": maxCorrectedFilesystemReadRate is " ?maxCorrectedFilesystemReadRate crlf)
	  (printout t "[filesystemCorrectedIo] " ?diskserverName ": maxCorrectedFilesystemWriteRate is " ?maxCorrectedFilesystemWriteRate crlf)
	)
	; We have found the maximum - we now weight v.s. it
        (loop-for-count (?index 1 ?diskserverNfs)
		(if (> ?maxCorrectedFilesystemReadRate 0.) then
		  (bind ?newCorrectedFilesystemReadRate
			(/ (nth$ ?index
				 ?correctedFilesystemReadRate)
			   ?maxCorrectedFilesystemReadRate)
		  )
		  (bind ?correctedFilesystemReadRate
			(replace$
			 ?correctedFilesystemReadRate
			 ?index
			 ?index
			 ?newCorrectedFilesystemReadRate
			 )
		  )
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " correctedFilesystemReadRate is "
			      (nth$ ?index ?correctedFilesystemReadRate) crlf)
		  )
		)
		(if (> ?maxCorrectedFilesystemWriteRate 0.) then
		  (bind ?newCorrectedFilesystemWriteRate
			(/ (nth$ ?index
				 ?correctedFilesystemWriteRate)
			   ?maxCorrectedFilesystemWriteRate)
		  )
		  (bind ?correctedFilesystemWriteRate
			(replace$
			 ?correctedFilesystemWriteRate
			 ?index
			 ?index
			 ?newCorrectedFilesystemWriteRate
			 )
		  )
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " correctedFilesystemWriteRate is "
			      (nth$ ?index ?correctedFilesystemWriteRate) crlf)
		  )
		)
		(if (< (length ?filesystemReadImportance) ?index)
		    then
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " filesystemReadImportance is "
			      ?*filesystemReadImportance* " (default)"
			      crlf)
		    )
		  (bind ?thisReadImportance ?*filesystemReadImportance*)
		  else
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " filesystemReadImportance is "
			      (nth$ ?index ?filesystemReadImportance)
			      crlf)
		    )
		  (bind ?thisReadImportance (nth$ ?index ?filesystemReadImportance))
		 )
		(if (< (length ?filesystemWriteImportance) ?index)
		    then
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " filesystemWriteImportance is "
			      ?*filesystemWriteImportance* " (default)"
			      crlf)
		    )
		  (bind ?thisWriteImportance ?*filesystemWriteImportance*)
		  else
		  (if (!= 0 ?*Debug*) then
		    (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			      (nth$ ?index ?filesystemName) " filesystemWriteImportance is "
			      (nth$ ?index ?filesystemWriteImportance)
			      crlf)
		    )
		  (bind ?thisWriteImportance (nth$ ?index ?filesystemWriteImportance))
		)
		(bind ?fFsIo (+ (* (nth$ ?index ?correctedFilesystemReadRate) ?thisReadImportance)
				(* (nth$ ?index ?correctedFilesystemWriteRate) ?thisWriteImportance)
				)
		)
		(if (!= 0 ?*Debug*) then
		  (printout t "[filesystemCorrectedIo] " ?diskserverName ":"
			    (nth$ ?index ?filesystemName)
			    "=> fFsIo is "
			    ?fFsIo crlf)
		  )
		(if (= 1 ?index)
		   then
		  (bind ?filesystemCorrectedIo (create$ ?fFsIo))
		  else
		  (bind ?filesystemCorrectedIo (insert$
							      ?filesystemCorrectedIo
							      (+ 1
								 (length ?filesystemCorrectedIo))
							      ?fFsIo))
		)
	)
	(send ?diskserver put-filesystemCorrectedIo ?filesystemCorrectedIo)
	(if (!= 0 ?*Debug*) then
	  (printout t "[filesystemCorrectedIo] setting filesystemCorrectedIoDone" crlf)
	)
	(send ?diskserver put-filesystemCorrectedIoDone 1)
)

; +++++++++++++++++++++++++++++++++++++++++++++++++++
; Compute corrected i/o over all diskserver instances
; +++++++++++++++++++++++++++++++++++++++++++++++++++
(defrule diskserverCorrectedIoMax
	?diskserver <- (object (is-a DISKSERVER)
			        (diskserverCorrectedIoMax ?diskserverCorrectedIoMax)
				(diskserverIo ?diskserverIo)
				(diskserverIoDone ?diskserverIoDone)
				(diskserverIoFactor ?diskserverIoFactor)
			)
	(test (< ?diskserverCorrectedIoMax 0))
	(test (= ?diskserverIoDone 1))
=>
        (bind ?CorrectedIoMax 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(bind ?thisCorrectedIo (* ?p:diskserverIo
						  ?p:diskserverIoFactor))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverCorrectedIoMax] "
			    ?p:diskserverName ": diskserverIo is "
			    ?p:diskserverIo ", diskserverIoFactor is "
			    ?p:diskserverIoFactor ", leaving to correctedIo " ?thisCorrectedIo crlf)
		)
		(if (> ?thisCorrectedIo ?CorrectedIoMax)
		    then
		  (bind ?CorrectedIoMax ?thisCorrectedIo)
		)
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverCorrectedIoMax] diskserverCorrectedIoMax is " ?CorrectedIoMax crlf)
	)
	(send ?diskserver put-diskserverCorrectedIoMax ?CorrectedIoMax)
)

; ++++++++++++++++++++++++++++++
; Compute weight of a diskserver
; ++++++++++++++++++++++++++++++
(defrule diskserverWeight
	?diskserver <- (object (is-a DISKSERVER)
				(diskserverName ?diskserverName)
				(diskserverCorrectedFreeMemMax ?diskserverCorrectedFreeMemMax)
				(diskserverCorrectedFreeRamMax ?diskserverCorrectedFreeRamMax)
				(diskserverCorrectedFreeSwapMax ?diskserverCorrectedFreeSwapMax)
				(diskserverCorrectedAvailProcMax ?diskserverCorrectedAvailProcMax)
				(diskserverCorrectedLoadMax ?diskserverCorrectedLoadMax)
				(diskserverCorrectedIoMax ?diskserverCorrectedIoMax)
				(diskserverFreeMem ?diskserverFreeMem)
				(diskserverFreeRam ?diskserverFreeRam)
				(diskserverFreeSwap ?diskserverFreeSwap)
				(diskserverAvailProc ?diskserverAvailProc)
				(diskserverLoad ?diskserverLoad)
				(diskserverIo ?diskserverIo)
				(diskserverMemFactor ?diskserverMemFactor)
				(diskserverRamFactor ?diskserverRamFactor)
				(diskserverSwapFactor ?diskserverSwapFactor)
				(diskserverProcFactor ?diskserverProcFactor)
				(diskserverLoadFactor ?diskserverLoadFactor)
				(diskserverIoFactor ?diskserverIoFactor)
				(diskserverMemImportance ?diskserverMemImportance)
				(diskserverRamImportance ?diskserverRamImportance)
				(diskserverSwapImportance ?diskserverSwapImportance)
				(diskserverProcImportance ?diskserverProcImportance)
				(diskserverLoadImportance ?diskserverLoadImportance)
				(diskserverIoImportance ?diskserverIoImportance)
				(readImportance ?readImportance)
				(writeImportance ?writeImportance)
				(readWriteImportance ?readWriteImportance)
				(filesystemNbRdonly $?filesystemNbRdonly)
				(filesystemNbWronly $?filesystemNbWronly)
				(filesystemNbReadWrite $?filesystemNbReadWrite)
				(filesystemName $?filesystemName)
				(filesystemCorrectedIo $?filesystemCorrectedIo)
				(diskserverTotalMem ?diskserverTotalMem)
				(diskserverTotalRam ?diskserverTotalRam)
				(diskserverTotalSwap ?diskserverTotalSwap)
				(diskserverNfs ?diskserverNfs)
				(diskserverStatus ?diskserverStatus)
				(filesystemReadRate $?filesystemReadRate)
				(filesystemWriteRate $?filesystemWriteRate)
				(filesystemStatus $?filesystemStatus)
				(diskserverFlag ?diskserverFlag)
				(diskserverClassFlag ?diskserverClassFlag)
			)
	; diskserver not yet weighted
	(test (= ?diskserverFlag 0))
	; But these values are available
	(test (>= ?diskserverCorrectedFreeMemMax 0))
	(test (>= ?diskserverCorrectedFreeRamMax 0))
	(test (>= ?diskserverCorrectedFreeSwapMax 0))
	(test (>= ?diskserverCorrectedAvailProcMax 0))
	(test (>= ?diskserverCorrectedLoadMax 0))
	(test (>= ?diskserverCorrectedIoMax 0))
=>
        ; Compute main node weight
        ; ++++++++++ fRam
	(if (> ?diskserverCorrectedFreeRamMax 0.) then
	  (bind ?fRam (/ (* ?diskserverFreeRam ?diskserverRamFactor) ?diskserverCorrectedFreeRamMax))
	 else
	  (bind ?fRam 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fRam is " ?fRam crlf)
	)
        ; ++++++++++ fMem
	(if (> ?diskserverCorrectedFreeMemMax 0.) then
	  (bind ?fMem (/ (* ?diskserverFreeMem ?diskserverMemFactor) ?diskserverCorrectedFreeMemMax))
	 else
	  (bind ?fMem 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fMem is " ?fMem crlf)
	)
        ; ++++++++++ fSwap
	(if (> ?diskserverCorrectedFreeSwapMax 0.) then
	  (bind ?fSwap (/ (* ?diskserverFreeSwap ?diskserverSwapFactor) ?diskserverCorrectedFreeSwapMax))
	 else
	  (bind ?fSwap 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fSwap is " ?fSwap crlf)
	)
        ; ++++++++++ fProc
	(if (> ?diskserverCorrectedAvailProcMax 0.) then
	  (bind ?fProc (/ (* ?diskserverAvailProc ?diskserverProcFactor) ?diskserverCorrectedAvailProcMax))
	 else
	  (bind ?fProc 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fProc is " ?fProc crlf)
	)
        ; ++++++++++ fLoad
	(if (> ?diskserverCorrectedLoadMax 0.) then
	  (bind ?fLoad (/ (* ?diskserverLoad ?diskserverLoadFactor) ?diskserverCorrectedLoadMax))
	 else
	  (bind ?fLoad 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fLoad is " ?fLoad crlf)
	)
	; ++++++++++ fIo
	(if (> ?diskserverCorrectedIoMax 0.) then
	  (bind ?fIo (/ (* ?diskserverIo ?diskserverIoFactor) ?diskserverCorrectedIoMax))
	 else
	  (bind ?fIo 0.)
	 )
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": fIo is " ?fIo crlf)
	)

	(bind ?newfRam  (* ?fRam  ?diskserverRamImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfRam is " ?newfRam crlf)
	)
	(bind ?newfMem  (* ?fMem  ?diskserverMemImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfMem is " ?newfMem crlf)
	)
	(bind ?newfSwap (* ?fSwap ?diskserverSwapImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfSwap is " ?newfSwap crlf)
	)
	(bind ?newfProc (* ?fProc ?diskserverProcImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfProc is " ?newfProc crlf)
	)
	(bind ?newfLoad (*  ?fLoad ?diskserverLoadImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfLoad is " ?newfLoad crlf)
	)
	(bind ?newfIo    (* ?fIo   ?diskserverIoImportance))
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": newfIo is " ?newfIo crlf)
	)
	; Total weight is the sum of newFxx...
	(bind ?diskserverWeight (+ (* ?*ramSign* ?newfRam)
				   (* ?*memSign* ?newfMem)
				   (* ?*swapSign* ?newfSwap)
				   (* ?*procSign* ?newfProc)
				   (* ?*loadSign* ?newfLoad)
				   (* ?*ioSign* ?newfIo))
	)
	(if (!= 0 ?*Debug*) then
	  (printout t "[diskserverWeight] " ?diskserverName ": diskserverWeight is " ?diskserverWeight crlf)
	)

	; Count the total number of streams on that diskserver
	(bind ?diskserverNbTot 0)
	(loop-for-count (?index 1 ?diskserverNfs)
		(bind ?diskserverNbTot (+
						?diskserverNbTot
						(nth$ ?index ?filesystemNbRdonly)
						(nth$ ?index ?filesystemNbWronly)
						(nth$ ?index ?filesystemNbReadWrite)
					)
		)
	)
	; Protection
	(if (= 0 ?diskserverNbTot) then
		(bind ?diskserverNbTot 1)
	)
	(if (= 0 ?diskserverIo) then
		(bind ?diskserverIo 1)
	)
	; Compute relative weight of that filesystem : (N(fs)/Ntot + IO(fs)/IOtot)
	; Note: historically we were distinguishing r/w/rw, but we moved to a model
	; where there weight does not depend on the stream direction.
 	(bind ?filesystemReadWeight (create$ ))
	(bind ?filesystemWriteWeight (create$ ))
	(bind ?filesystemReadWriteWeight (create$ ))
	(loop-for-count (?index 1 ?diskserverNfs)
		(bind ?thisFilesystemNbRdonly (nth$ ?index ?filesystemNbRdonly))
		(bind ?thisFilesystemNbWronly (nth$ ?index ?filesystemNbWronly))
		(bind ?thisFilesystemNbReadWrite (nth$ ?index ?filesystemNbReadWrite))
		(bind ?thisFilesystemReadRate (nth$ ?index ?filesystemReadRate))
		(bind ?thisFilesystemWriteRate (nth$ ?index ?filesystemWriteRate))
		(bind ?thisFilesystemNbTot
		      (+ ?thisFilesystemNbRdonly
			 ?thisFilesystemNbWronly
			 ?thisFilesystemNbReadWrite)
		)
		(bind ?thisFilesystemIoTot
		      (+ ?thisFilesystemReadRate
			 ?thisFilesystemWriteRate)
		)
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverWeight] " ?diskserverName
			    ":"
			    (nth$ ?index ?filesystemName)
			    " Rdonly/Wronly/ReadWrite/IoRead/IoWrite -> NStreams/Io is "
			    ?thisFilesystemNbRdonly
			    "/"
			    ?thisFilesystemNbWronly
			    "/"
			    ?thisFilesystemNbReadWrite
			    "/"
			    ?thisFilesystemReadRate
			    "/"
			    ?thisFilesystemWriteRate
			    " -> "
			    ?thisFilesystemNbTot
			    "/"
			    ?thisFilesystemIoTot
			    crlf
		   )
		)
		(bind ?thisWeight
			(* ?diskserverWeight
				(+
					(/ ?thisFilesystemNbTot ?diskserverNbTot )
					(/ ?thisFilesystemIoTot ?diskserverIo    )
				)
			)
		 )
		(if (< ?thisWeight 0.1) then
			(bind ?thisWeight 0.1)
		)
		(bind ?thisReadWeight ?thisWeight)
		(bind ?thisWriteWeight ?thisWeight)
		(bind ?thisReadWriteWeight ?thisWeight)
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverWeight] " ?diskserverName
			    ":"
			    (nth$ ?index ?filesystemName )
			    " Rdonly/Wronly/ReadWrite weigth is "
			    ?thisReadWeight
			    "/"
			    ?thisWriteWeight
			    "/"
			    ?thisReadWriteWeight
			    crlf
		   )
		)
		; We can now use factor of importance and the sign for each type of stream
		(bind ?thisReadWeight (* ?thisReadWeight ?readImportance))
		(bind ?thisWriteWeight (* ?thisWriteWeight ?writeImportance))
		(bind ?thisReadWriteWeight (* ?thisReadWriteWeight ?readWriteImportance))
		(if (!= 0 ?*Debug*) then
		  (printout t "[diskserverWeight] " ?diskserverName
			    ":"
			    (nth$ ?index ?filesystemName )
			    " Rdonly/Wronly/ReadWrite factorised weigth is "
			    ?thisReadWeight
			    "/"
			    ?thisWriteWeight
			    "/"
			    ?thisReadWriteWeight
			    ", newfIo is "
			    ?newfIo
			    crlf
		   )
		)
		(if (= 1 ?index)
		 then
			(bind ?filesystemReadWeight (create$ ?thisReadWeight))
			(bind ?filesystemWriteWeight (create$ ?thisWriteWeight))
			(bind ?filesystemReadWriteWeight (create$ ?thisReadWriteWeight))
		 else
			(bind ?filesystemReadWeight (insert$ ?filesystemReadWeight (+ 1 (length ?filesystemReadWeight)) ?thisReadWeight))
			(bind ?filesystemWriteWeight (insert$ ?filesystemWriteWeight (+ 1 (length ?filesystemWriteWeight)) ?thisReadWeight))
			(bind ?filesystemReadWriteWeight (insert$ ?filesystemReadWriteWeight (+ 1 (length ?filesystemReadWriteWeight)) ?thisReadWeight))
		)
	)
	(if (!= 0 ?*Debug*) then
	    (printout t "[diskserverWeight] " ?diskserverName
			    ": Weight of read streams is "
			    ?filesystemReadWeight crlf)
	    (printout t "[diskserverWeight] " ?diskserverName
			    ": Weight of write streams is "
			    ?filesystemWriteWeight crlf)
	    (printout t "[diskserverWeight] " ?diskserverName
			    ": Weight of read/write streams is "
			    ?filesystemReadWriteWeight crlf)
	  )
	
	(bind ?change 0)
	(send ?diskserver put-diskserverFlag 1)
	(send ?diskserver put-filesystemReadWeight ?filesystemReadWeight)
	(send ?diskserver put-filesystemWriteWeight ?filesystemWriteWeight)
	(send ?diskserver put-filesystemReadWriteWeight ?filesystemReadWriteWeight)

	; Check if there is still at least one diskserverWeight rule
	; not yet performed
	(bind ?Ntodo 0)
	(do-for-all-instances ((?p DISKSERVER)) TRUE
		(if (= ?p:diskserverFlag 0) then
			(bind ?Ntodo 1)
			(break)
		)
	)
	; (send ?diskserver put-diskserverWeight (+ 1. ?diskserverWeight))
	(send ?diskserver put-diskserverWeight ?diskserverWeight)
	(if (= ?Ntodo 0) then
		; We use this instance to fire the rule on the files...
		(send ?diskserver put-diskserverClassFlag 1)
		; And we prepare the final values for the final sort:
		; We have the diskserverWeight and the weight each
		; stream would take if we were to assign it
		; That is the globalWeight of a filesystem is
		; diskserverWeight + filesystemReadWeight in rdonly mode
		; diskserverWeight + filesystemWriteWeight in wronly mode
		; diskserverWeight + filesystemReadWriteWeight in rdonly/wronly mode
		(do-for-all-instances ((?p DISKSERVER)) TRUE
			(loop-for-count (?index 1 ?p:diskserverNfs)
				(make-instance of FILESYSTEM
					(diskserverInstance
					 (instance-address ?p))
					(filesystemIndex ?index)
				)
			)
		)
	)
)

; --------------------------
; Plugin for sort over files
; --------------------------
(deffunction fileSortAtime> (?a ?b)
  (> (send ?a get-statAtime) (send ?b get-statAtime))
)

(deffunction fileSortAtimeReverse (?a ?b)
  (< (send ?a get-statAtime) (send ?b get-statAtime))
)

; ------------------------------------
; Plugin for sort over all filesystems
; ------------------------------------
(deffunction filesystemWeight> (?a ?b)
  (bind ?diskWeightA (send ?a get-diskserverWeight))
  (bind ?fsWeightA (send ?a get-filesystemWeight))
  (bind ?weightA (* ?diskWeightA (+ 1. ?fsWeightA)))
  (if (!= 0 ?*Debug*) then
    (printout t "[filesystemWeight>] " (send ?a get-diskserverName)
	      ":" (send ?a get-filesystemName)
	      ", diskWeightA=" ?diskWeightA " * (1 + fsWeightA=" ?fsWeightA
	      ") = " ?weightA crlf)
  )
  (bind ?diskWeightB (send ?b get-diskserverWeight))
  (bind ?fsWeightB (send ?b get-filesystemWeight))
  (bind ?weightB (* ?diskWeightB (+ 1. ?fsWeightB)))
  (if (!= 0 ?*Debug*) then
    (printout t "[filesystemWeight>] " (send ?a get-diskserverName)
	      ":" (send ?a get-filesystemName)
	      ", diskWeightB=" ?diskWeightB " * (1 + fsWeightB=" ?fsWeightB
	      ") = " ?weightB crlf)
  )
  (> ?weightA ?weightB)
)

; -----------------------------------------------
; Returns the weight of a filesystem + diskserver
; -----------------------------------------------
(deffunction filesystemWeight_lt (?a ?b)
  (bind ?diskWeightA (send ?a get-diskserverWeight))
  (bind ?fsWeightA (send ?a get-filesystemWeight))
  (bind ?weightA (* ?diskWeightA (+ 1. ?fsWeightA)))
  (if (!= 0 ?*Debug*) then
    (printout t "[filesystemWeight>] " (send ?a get-diskserverName)
	      ":" (send ?a get-filesystemName)
	      ", diskWeightA=" ?diskWeightA " * (1 + fsWeightA=" ?fsWeightA
	      ") = " ?weightA crlf)
  )
  (bind ?diskWeightB (send ?b get-diskserverWeight))
  (bind ?fsWeightB (send ?b get-filesystemWeight))
  (bind ?weightB (* ?diskWeightB (+ 1. ?fsWeightB)))
  (if (!= 0 ?*Debug*) then
    (printout t "[filesystemWeight>] " (send ?a get-diskserverName)
	      ":" (send ?a get-filesystemName)
	      ", diskWeightB=" ?diskWeightB " * (1 + fsWeightB=" ?fsWeightB
	      ") = " ?weightB crlf)
  )
  (< ?weightA ?weightB)
)

; ---------------------------
; Returns the best filesystem
; ---------------------------
(deffunction bestFs ()
  (first$ (sort filesystemWeight_lt (find-all-instances ((?filesystem FILESYSTEM)) (= 1 ?filesystem:flag))))
)

; ---------------------------------------------
; Returns the oldest file on a given filesystem
; ---------------------------------------------

(deffunction newestFile (?fs)
  (return (nth$ 1 (first$ (sort fileSortAtime (find-all-instances ((?file FILE)) (and (= 0 (str-compare ?fs ?file:diskcopyFs)) (< ?file:fileFlag 0)))))))
)


(deffunction oldestFile (?fs)
  (return (nth$ 1 (first$ (sort fileSortAtimeReverse (find-all-instances ((?file FILE)) (and (= 0 (str-compare ?fs ?file:diskcopyFs)) (< ?file:fileFlag 0)))))))
)

; --------------------------------------------------------------------
; Returns TRUE if there is at least one file on a given filesystem not
; yet assigned
; --------------------------------------------------------------------
(deffunction hasFileOnFs (?fs)
  (any-instancep ((?file FILE)) (and (= 0 (str-compare ?fs ?file:diskcopyFs)) (< ?file:fileFlag 0)))
)
