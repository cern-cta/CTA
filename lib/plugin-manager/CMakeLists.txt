# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctapluginmanager INTERFACE)

add_public_headers(ctapluginmanager)
target_link_libraries(ctapluginmanager INTERFACE ${CMAKE_DL_LIBS})
