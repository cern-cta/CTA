# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required(VERSION 3.17)

#
# libctarmc
#

set(CTARMC_LIB_SRC_FILES
  Cdomainname.cpp
  Cglobals.cpp
  Cinitdaemon.cpp
  Cnetdb.cpp
  getconfent.cpp
  marshall.cpp
  serror.cpp
  socket.cpp
  socket_timeout.cpp
  strerror_r_wrapper.cpp
  spectra_like_libs.cpp)

add_library(ctarmc ${CTARMC_LIB_SRC_FILES})
