{{- define "oracleUnitTest.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "oracleUnitTest.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "oracleUnitTest.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}