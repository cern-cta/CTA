/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueFactory.hpp"

#include <string>

/** Command-line arguments of cta-unit-tests-rdbms. */
struct RdbmsUnitTestsCmdLineArgs {
  /** Path to the file containing the database connection details. */
  std::string dbConfigPath;
};

/** Arguments supplied by the database runner for use by its test suites. */
extern RdbmsUnitTestsCmdLineArgs g_cmdLineArgs;

/** Catalogue factory owned by the database runner and borrowed by its test suites. */
extern cta::catalogue::CatalogueFactory* g_catalogueFactoryForUnitTests;
