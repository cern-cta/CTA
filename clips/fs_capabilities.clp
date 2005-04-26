(deffunction maxIo (?filesystemName)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c1-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c2-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c3-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c4-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c5-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c6-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c7-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c8-1/")) then
		(return 60.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c9-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c10-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c11-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c12-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c13-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c14-1/")) then
		(return 110.)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c15-1/")) then
		(return 110.)
	)
	(return 0.)
)

(deffunction maxNbFd (?filesystemName)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c1-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c2-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c3-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c4-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c5-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c6-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c7-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c8-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c9-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c10-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c11-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c12-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c13-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c14-1/")) then
		(return 10)
	)
	(if (= 0 (str-compare ?filesystemName "/tank/cern.ch/c-filesets/c15-1/")) then
		(return 10)
	)
        (return 0)
)

