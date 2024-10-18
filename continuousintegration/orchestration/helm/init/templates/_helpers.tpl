{{- define "init.name" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "init.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.tapeConfig" -}}
  {{- $tapeConfig := .Values.tapeConfig }}
  {{- if ne (typeOf $tapeConfig) "string" -}}
    {{- $tapeConfig = $tapeConfig | toYaml }}
  {{- end }}
  {{- $tapeConfig -}}
{{- end }}
