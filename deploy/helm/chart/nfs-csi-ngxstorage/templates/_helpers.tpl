{{- define "ngxstorage.driverName" -}}
{{- $prefix := toString (.Values.driverNamePrefix | default "") | trimSuffix "." | lower -}}
{{- if $prefix -}}{{- $prefix -}}.{{- end -}}nfs.csi.ngxstorage.com
{{- end -}}

{{- define "ngxstorage.fullname" -}}
{{- $prefix := toString (.Values.driverNamePrefix | default "") | trimSuffix "-" | lower -}}
{{- if $prefix -}}{{- printf "%s-nfs-csi-ngxstorage" $prefix -}}{{- else -}}nfs-csi-ngxstorage{{- end -}}
{{- end -}}
