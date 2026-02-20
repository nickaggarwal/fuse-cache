{{- define "fuse-cache.namespace" -}}
{{- .Values.namespace.name -}}
{{- end -}}

{{- define "fuse-cache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fuse-cache.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fuse-cache.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
