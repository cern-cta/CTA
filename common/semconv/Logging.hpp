/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

// Would be nice to merge this with Attributes. However, at the moment the naming conventions are different
// Attributes uses dots for namespacing, logs use underscores
// Hopefully once we go to log schema 1.0.0 we can update the logs to also use the dots naming convention
namespace cta::semconv::log {

// -------------------- Attribute Keys --------------------

// https://opentelemetry.io/docs/specs/semconv/registry/attributes/exception/#exception-attributes
static constexpr const char* exceptionMessage = "exception_message";

// https://opentelemetry.io/docs/specs/semconv/registry/attributes/exception/#exception-attributes
static constexpr const char* exceptionType = "exception_type";

// https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-attributes
// Note that error_messages is deprecated and the recommendation is to use domain-specific error messages
static constexpr const char* errorMessage = "error_message";

}  // namespace cta::semconv::log
