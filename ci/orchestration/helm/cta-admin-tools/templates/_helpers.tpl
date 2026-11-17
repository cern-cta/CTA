{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "cta-admin-tools.client.fullname" -}}
  {{ include "common.names.fullname" (dict "Values" .Values.client "Release" .Release "Chart" .Chart) }}
{{- end }}

