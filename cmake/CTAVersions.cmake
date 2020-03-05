# Default CTA Version
set(CTA_VERSION 0)
set(CTA_RELEASE 1)

# Catalogue Schema Version
set(CTA_CATALOGUE_SCHEMA_VERSION_MAJOR 2)
set(CTA_CATALOGUE_SCHEMA_VERSION_MINOR 0)

# Shared object internal version (used in SONAME)
set(CTA_SOVERSION 0)

# Shared object external version (used in filename)
set(CTA_SOMAJOR ${CTA_SOVERSION})
set(CTA_SOMINOR 1)
set(CTA_SOPATCH 0)

# Get version number from environment if set.
if(NOT $ENV{CTA_VERSION} STREQUAL "")
  set(CTA_VERSION $ENV{CTA_VERSION})
  message(STATUS "Got CTA_VERSION from environment: ${CTA_VERSION}")
else(NOT $ENV{CTA_VERSION} STREQUAL "")
  message(STATUS "Using default CTA_VERSION: ${CTA_VERSION}")
endif(NOT $ENV{CTA_VERSION} STREQUAL "")

# Get release number from environment if set
if(NOT $ENV{CTA_RELEASE} STREQUAL "")
  set(CTA_RELEASE $ENV{CTA_RELEASE})
  message(STATUS "Got CTA_RELEASE from environment: ${CTA_RELEASE}")
else(NOT $ENV{CTA_RELEASE} STREQUAL "")
  message(STATUS "Using default CTA_RELEASE: ${CTA_RELEASE}")
endif(NOT $ENV{CTA_RELEASE} STREQUAL "")

# Change the release number if VCS version is provided
if(DEFINED VCS_VERSION)
  set(CTA_RELEASE ${VCS_VERSION})
  message(STATUS "Replaced CTA_RELEASE with VCS_VERSION: ${CTA_RELEASE}")
endif(DEFINED VCS_VERSION)

message(STATUS "CTA version is ${CTA_VERSION}-${CTA_RELEASE}")

# Shared library versioning
set(CTA_LIBVERSION ${CTA_SOMAJOR}.${CTA_SOMINOR}.${CTA_SOPATCH})
message(STATUS "CTA shared object version is ${CTA_LIBVERSION} (${CTA_SOVERSION})")

