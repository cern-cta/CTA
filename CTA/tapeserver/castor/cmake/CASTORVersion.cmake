# Guess the CASTOR version from the various criteria in the environement:
# - Get MINOR_CASTOR_VERSION and MAJOR_CASTOR_VERSION from the environement, if 
# they exists.
# - If not, guess them from the file debian/changelog
# - The major is expected in the form: A.B (fixed to 2.1 for ages)
# - The minor is expected to be in the for C.D.
# - The package version (in rpm) will be A.B.C-D
# - If the variable VCS_VERSION is defined (from command line:
#  "cmake -DVCS_VERSION:STRING=git-6789abc")

# Extract the versions from changelog anyway
set(VERSION_RE "^castor \\(([0-9]+)\\.([0-9]+)\\.([0-9]+)-([0-9]+)\\).*")
file(STRINGS debian/changelog CHANGELOG_HEAD
     REGEX ${VERSION_RE}
     LIMIT_COUNT 1)
# message("Changelog line: ${CHANGELOG_HEAD}")
# message("VERSION_RE ${VERSION_RE}")
string(REGEX REPLACE "${VERSION_RE}" "\\1" A ${CHANGELOG_HEAD})
string(REGEX REPLACE "${VERSION_RE}" "\\2" B ${CHANGELOG_HEAD})
string(REGEX REPLACE "${VERSION_RE}" "\\3" C ${CHANGELOG_HEAD})
string(REGEX REPLACE "${VERSION_RE}" "\\4" D ${CHANGELOG_HEAD})
# message("A ${A} B ${B} C ${C} D ${D}")

# Get versions from environment, if availble
# MAJOR
if(NOT $ENV{MAJOR_CASTOR_VERSION} STREQUAL "")
  set(MAJOR_CASTOR_VERSION $ENV{MAJOR_CASTOR_VERSION})
  message(STATUS "Got MAJOR_CASTOR_VERSION from environment: ${MAJOR_CASTOR_VERSION}")
else(NOT $ENV{MAJOR_CASTOR_VERSION} STREQUAL "")
  set(MAJOR_CASTOR_VERSION "${A}.${B}")
  message(STATUS "Got MAJOR_CASTOR_VERSION from debian/changelog: ${MAJOR_CASTOR_VERSION}")
endif(NOT $ENV{MAJOR_CASTOR_VERSION} STREQUAL "")

# MINOR
if(NOT $ENV{MINOR_CASTOR_VERSION} STREQUAL "")
  set(MINOR_CASTOR_VERSION $ENV{MINOR_CASTOR_VERSION})
  message(STATUS "Got MINOR_CASTOR_VERSION from environment: ${MINOR_CASTOR_VERSION}")
else(NOT $ENV{MINOR_CASTOR_VERSION} STREQUAL "")
  set(MINOR_CASTOR_VERSION "${C}.${D}")
  message(STATUS "Got MINOR_CASTOR_VERSION from debian/changelog: ${MINOR_CASTOR_VERSION}")
endif(NOT $ENV{MINOR_CASTOR_VERSION} STREQUAL "")

# Change the release number if VCS version is provided
if(DEFINED VCS_VERSION)
  STRING(REGEX REPLACE "\\..*" ".${VCS_VERSION}" MINOR_CASTOR_VERSION ${MINOR_CASTOR_VERSION})
  message(STATUS "Replaced VCS_VERSION in MINOR_CASTOR_VERSION: ${MINOR_CASTOR_VERSION}")
endif(DEFINED VCS_VERSION)

# Generate derived version from result. Almost all combinaisons are needed at some point!
string(REGEX REPLACE "(.*)\\..*" "${MAJOR_CASTOR_VERSION}.\\1" CASTOR_VERSION ${MINOR_CASTOR_VERSION})
string(REGEX REPLACE ".*\\." "" CASTOR_RELEASE ${MINOR_CASTOR_VERSION})
string(REGEX REPLACE "\\..*" "" MAJOR_CASTOR_VERSION_TOP ${MAJOR_CASTOR_VERSION})
string(REGEX REPLACE ".*\\." "" MAJOR_CASTOR_VERSION_BOTTOM ${MAJOR_CASTOR_VERSION})
string(REGEX REPLACE "\\..*" "" MINOR_CASTOR_VERSION_TOP ${MINOR_CASTOR_VERSION})
string(REGEX REPLACE ".*\\." "" MINOR_CASTOR_VERSION_BOTTOM ${MINOR_CASTOR_VERSION})
set(CASTOR_DB_VERSION "${MAJOR_CASTOR_VERSION_TOP}_${MAJOR_CASTOR_VERSION_BOTTOM}_${MINOR_CASTOR_VERSION_TOP}_${MINOR_CASTOR_VERSION_BOTTOM}")
message(STATUS "Generated derived versions: CASTOR_VERSION: ${CASTOR_VERSION} CASTOR_RELEASE: ${CASTOR_RELEASE} CASTOR_DB_VERSION: ${CASTOR_DB_VERSION}")
message(STATUS "MAJOR_CASTOR_VERSION_TOP=${MAJOR_CASTOR_VERSION_TOP} MAJOR_CASTOR_VERSION_BOTTOM=${MAJOR_CASTOR_VERSION_BOTTOM}")
message(STATUS "MINOR_CASTOR_VERSION_TOP=${MINOR_CASTOR_VERSION_TOP} MINOR_CASTOR_VERSION_BOTTOM=${MINOR_CASTOR_VERSION_BOTTOM}")
