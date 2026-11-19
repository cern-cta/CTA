/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tests/runners/rdbms/RdbmsTestContext.hpp"

RdbmsUnitTestsCmdLineArgs g_cmdLineArgs;
cta::catalogue::CatalogueFactory* g_catalogueFactoryForUnitTests = nullptr;
