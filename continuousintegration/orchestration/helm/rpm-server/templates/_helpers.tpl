{{- define "rpmserver.name" -}}
{{- if .Values.fullnameOverride -}}
  {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
  {{- $name := default .Chart.Name .Values.nameOverride -}}
  {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end }}
{{- end }}

{{- define "rpmserver.image" -}}
  {{- printf "%s:%s" .Values.image.repository ( .Values.image.tag | toString ) | quote -}}
{{- end }}

{{- define "rpmserver.imagePullPolicy" -}}
  {{ .Values.image.pullPolicy | quote }}
{{- end }}

{{- define "rpmserver.imagePullSecrets" -}}
  {{- if .Values.image.pullSecrets -}}
    {{- range .Values.image.pullSecrets }}
- name: {{ . | quote }}
    {{- end -}}
  {{- end -}}
{{- end }}
