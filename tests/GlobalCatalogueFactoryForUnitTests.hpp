/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueFactory.hpp"

/**
 * Declaration of the global variable used to point to a factory of CTA
 * catalogue objects.
 */
extern cta::catalogue::CatalogueFactory *g_catalogueFactoryForUnitTests;
