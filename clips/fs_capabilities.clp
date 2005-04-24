(deffunction maxIo (?filesystemName)
	(bind ?thisIo 0.)
	(if (= 0 (str-compare ?filesystemName "/shift/lxfsrm505/data01/")) then
		(return 70.)
	)
	(return 0.)
)
