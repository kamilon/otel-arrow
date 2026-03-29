# Contrib Nodes

This crate contains optional (feature-gated) contrib receivers, processors, and exporters.

## Folder Layout

- `src/receivers/`
  - Contrib receivers
- `src/processors/`
  - Contrib processors
  - `src/exporters/`
  - Contrib exporters

## Features

Feature flags are grouped into aggregate categories and individual node flags.
Aggregate flags enable all nodes in their category.

### Receivers

- `contrib-receivers` (enables all contrib receivers)

| Feature | Enables Node | Node URN | Module |
| ------- | ------------ | -------- | ------ |
| `file-tail-receiver` | File Tail receiver | `urn:otel:receiver:file_tail` | `src/receivers/file_tail/` |

### Processors

- `contrib-processors` (enables all contrib processors)

| Feature | Enables Node | Node URN | Module |
| ------- | ------------ | -------- | ------ |
| `condense-attributes-processor` | Condense Attributes processor | `urn:otel:processor:condense_attributes` | `src/processors/condense_attributes_processor/` |
| `recordset-kql-processor` | RecordSet KQL processor | `urn:microsoft:processor:recordset_kql` | `src/processors/recordset_kql_processor/` |
| `resource-validator-processor` | Resource Validator processor | `urn:otel:processor:resource_validator` | `src/processors/resource_validator_processor/` |

### Exporters

- `contrib-exporters` (enables all contrib exporters)

| Feature | Enables Node | Node URN | Module |
| ------- | ------------ | -------- | ------ |
| `geneva-exporter` | Geneva exporter | `urn:microsoft:exporter:geneva` | `src/exporters/geneva_exporter/` |
| `azure-monitor-exporter` | Azure Monitor exporter | `urn:microsoft:exporter:azure_monitor` | `src/exporters/azure_monitor_exporter/` |

When these features are enabled in the top-level binary, their factories are
registered into the OTAP pipeline factory maps.
