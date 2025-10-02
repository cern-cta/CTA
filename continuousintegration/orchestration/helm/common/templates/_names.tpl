// SPDX-FileCopyrightText: 2025 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

{{/*
   names.tpl
   This file defines common naming conventions for resources in the Helm chart.
   It includes helpers for generating full names, handling name overrides, and truncating names
   to comply with Kubernetes' 63-character limit.
*/}}

{{/*
   Generate the full name for a resource, handling name overrides and ensuring the final name is
   truncated to 63 characters, as Kubernetes requires.
   - If .Values.fullnameOverride is provided, it is used as the full name.
   - If .Values.nameOverride is provided, the release name is combined with the nameOverride.
   - Otherwise, the name is generated from the release name and chart name.
*/}}
{{- define "common.names.fullname" -}}
{{- if .Values.fullnameOverride -}}
  {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
  {{- $name := default .Chart.Name .Values.nameOverride -}}
  {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end }}
{{- end }}

{{/*
   Generate a name for a resource.
*/}}
{{- define "common.names.name" -}}
{{- if .Values.nameOverride -}}
  {{- .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
  {{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}
{{- end }}

{{/*
   Generate the release-specific name for a resource.
   This is a shorter name that combines the release name and chart name but is truncated to ensure it complies
   with Kubernetes' name limitations.
*/}}
{{- define "common.names.release" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}
