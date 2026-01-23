{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "common.tplvalues.render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}

{{/*
Taken from https://github.com/helm/helm/issues/3001#issuecomment-2691953317
Because of Helm bug (https://github.com/helm/helm/issues/3001), Helm converts
int value to float64 implictly, like 2748336 becomes 2.748336e+06.
This breaks the output even when using quote to render.

Use this function when you want to get the string value only.
It handles the case when the value is string itself as well.
Parameters: is string/number
*/}}
{{- define "common.stringOrNumber" -}}
{{- if kindIs "string" . }}
  {{- print . -}}
{{- else  }}
  {{- int64 . | toString -}}
{{- end -}}
{{- end -}}
