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

{{- define "init.libraries" -}}
  {{- $libraryConfig := .Values.libraries }}
  {{- if ne (typeOf $libraryConfig) "string" -}}
    {{- $libraryConfig = $libraryConfig | toYaml }}
  {{- end }}
  {{- $libraryConfig -}}
{{- end }}

{{- define "init.mhvtlLibraryDevices" -}}
{{- $filteredDevices := list -}}
{{- range (include "tpsrv.libraries" . | fromYaml) }}
  {{- if eq .libraryType "mhvtl" }}
    {{- $filteredDevices = append $filteredDevices .libraryDevice }}
  {{- end }}
{{- end }}
{{- join " " $filteredDevices -}}
{{- end }}
