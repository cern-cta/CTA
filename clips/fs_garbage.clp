; $Id: fs_garbage.clp,v 1.1 2005/06/25 16:16:08 jdurand Exp $

(deffunction maxPercentFreeSpace (?filesystemName)
	; Default high threshold for garbage collection
	return 90.
)
(deffunction minPercentFreeSpace (?filesystemName)
	; Default minimm threshold for garbage collection
	return 80.
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
