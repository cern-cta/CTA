; $Id: fs_garbage.clp,v 1.2 2005/06/27 21:31:41 jdurand Exp $

(deffunction maxPercentFreeSpace (?filesystemName)
	; Default high threshold for garbage collection
	return 15.
)
(deffunction minPercentFreeSpace (?filesystemName)
	; Default minimm threshold for garbage collection
	return 5.
)

; =================================================================
; Functions that will return absolute maxFreeSpace and minFreeSpace
; =================================================================

(deffunction maxFreeSpace (?a)
	(bind ?maxPercentFreeSpace (maxPercentFreeSpace (send ?a get-filesystemName)))
	; Note - using div guarantees that the output is an integer
	return(div (* (send ?a get-filesystemTotalSpace) ?maxPercentFreeSpace) 100)
)
(deffunction minFreeSpace (?a)
	(bind ?minPercentFreeSpace (minPercentFreeSpace (send ?a get-filesystemName)))
	; Note - using div guarantees that the output is an integer
	return(div (* (send ?a get-filesystemTotalSpace) ?minPercentFreeSpace) 100)
)
