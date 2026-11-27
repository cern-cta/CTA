{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{/* Select workloads by release and role; chart and image updates must not change selectors. */}}
{{- define "cta-tools.selectorLabels" -}}
app.kubernetes.io/instance: {{ .root.Release.Name | quote }}
app.kubernetes.io/component: {{ .component | quote }}
{{- end }}

{{- define "cta-tools.labels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name | quote }}
{{ include "common.labels.withoutname" (dict "Values" (dict "component" .component) "Release" .root.Release "Chart" .root.Chart) }}
{{- end }}

