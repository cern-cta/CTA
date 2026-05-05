{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "kdc.fullname" -}}
  {{- printf "%s-%s" .Release.Name "kdc" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
