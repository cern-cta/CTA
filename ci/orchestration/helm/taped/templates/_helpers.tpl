{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "taped.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "taped.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "taped.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "taped.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "taped.slugify" -}}
{{- . | lower | regexReplaceAll "[^a-z0-9-]+" "-" | trimAll "-" | trunc 63 -}}
{{- end -}}
