# OTLP protos

`opentelemetry/proto/{common,resource,logs}/v1/*.proto` and
`opentelemetry/proto/collector/logs/v1/logs_service.proto` of
[opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto) v1.11.1, unmodified. Only
the logs service is served: the exporter posts `ExportLogsServiceRequest` over OTLP/HTTP.

Vendored instead of depending on the `opentelemetry-proto` crate, which pins its own prost / tonic and
pulls the SDK.

Apache License 2.0; see `LICENSE`.
