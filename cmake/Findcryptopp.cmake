# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     CRYPTOPP_FOUND
#     CRYPTOPP_INCLUDE_DIRS
#     CRYPTOPP_LIBRARIES

find_path(CRYPTOPP_INCLUDE_DIRS
  base64.h
  PATH_SUFFIXES cryptopp)

find_library(CRYPTOPP_LIBRARIES
  NAMES cryptopp)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cryptopp DEFAULT_MSG
  CRYPTOPP_INCLUDE_DIRS CRYPTOPP_LIBRARIES)
