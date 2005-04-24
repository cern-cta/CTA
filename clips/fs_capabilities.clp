; Please put maxio per filesystem but not the theorical value, rather
; something below, for example 40 MB/S here the max theor. value is 50.
; 110 instead of 120, etc...
; Otherwise, since we will not /always/ reach the max value, the maxio
; will have no effect

(deffunction maxIo (?filesystemName)
	(if (= 0 (str-compare ?filesystemName "/shift/lxfsrm505/data01/")) then
		(return 30.)
	)
	(return 0.)
)

(deffunction maxNbFd (?filesystemName)
	(if (= 0 (str-compare ?filesystemName "/shift/lxfsrm505/data01/")) then
		(return 5)
	)
	(return 0)
)
