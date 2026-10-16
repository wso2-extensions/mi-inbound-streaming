# Streaming File Inbound Endpoint for WSO2 Micro Integrator 4.1.0

The streaming file inbound endpoint lets **WSO2 Micro Integrator 4.1.0** process very large files (multi-gigabyte CSV, JSON, JSON Lines and text files) without loading them into memory. It reads a file as a stream and injects it into a sequence **one record or one chunk of records at a time**, so a file far larger than the heap passes through with bounded memory.

On top of streaming, it adds the things a long-running file job needs: resume after a restart, per-record error handling that does not fail the whole file, parallel mediation, an output (reply) file, and callbacks when a file is finished.

> **Supported runtime: WSO2 Micro Integrator 4.1.0.** Built and tested against MI 4.1.0 (Synapse `2.1.7-wso2v271`, commons-vfs `2.2.0-wso2v13.20`). It runs on the JDK that MI 4.1.0 runs on, including JDK 8.

## Contents

- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [How a file is processed](#how-a-file-is-processed)
- [Configuration reference](#configuration-reference)
- [Message context properties](#message-context-properties)
- [Parallel mediation](#parallel-mediation)
- [Checkpointing and resume](#checkpointing-and-resume)
- [Error handling](#error-handling)
- [Samples](#samples)
- [Limitations on MI 4.1.0](#limitations-on-mi-410)
- [Building from source](#building-from-source)
- [License](#license)

## Features

- **Streaming modes**
  - `RECORD`: one message per record.
  - `CHUNK`: one message per batch of N records.
  - `ENTIRE_FILE`: the classic one-message-per-file behaviour.
- **Input formats:** CSV, JSON (with a JSONPath selecting the repeating element), JSON Lines and plain text (one line per record).
- **CSV parsing:** custom delimiter and quote character, header handling, and **per-column JSON data types** (Integer, Number, Boolean, String), so a sequence can do arithmetic on CSV values.
- **Flexible output:** the parsed record or chunk becomes the message body, or is placed in a message property as a JSON string (MI 4.1.0 has no message variables).
- **Reply file:** what the sequence leaves in the message is appended to an output file, written as JSON Lines for JSON results and through the message formatter otherwise.
- **Per-record error handling:**
  - A record that fails to parse, or fails mediation, is moved to an error file or dropped.
  - The rest of the file carries on.
  - Only structural or I/O errors fail a whole file.
- **Checkpointing and resume:**
  - Progress is saved to the MI registry, so after a restart, crash or node failover a file resumes near where it stopped instead of from row 1.
  - Files are matched by their content, not their timestamp, so a resume also works across a cluster that shares the registry.
  - The checkpoint is kept until the file has been moved away.
- **Resilient remote reads:** if the connection drops mid-file (for example SFTP `Pipe closed`), the file is reopened and reading continues at the exact byte where it stopped. No record is lost or repeated.
- **Parallel mediation:**
  - Up to N records or chunks of a file are mediated concurrently.
  - The reply file, error files and checkpoint still follow file order.
- **File completion callbacks:** an optional sequence runs when a file finishes, and another when it fails. Both receive the file's totals.
- **Position properties:** every message carries its record, chunk and row-range numbers.
- **Cheap "still being written" check:** files are held back while their size or modification time is changing, without reading their content.
- **Remote sources:**
  - Local files, FTP/FTPS, SFTP (with read and connect timeouts) and SMB2.
  - File locking for clustered consumption, with automatic release of stale locks.
  - Move or delete after processing.

## Installation

1. **Copy two jars into `<MI_HOME>/lib`:**

   | Jar | Notes |
   |---|---|
   | `mi-inbound-streaming-<version>.jar` | This inbound endpoint (from the release zip or `target/` after a build). |
   | `commons-csv-1.14.1.jar` | `org.apache.commons:commons-csv:1.14.1`. MI 4.1.0 does not ship it. |

   ```bash
   mvn dependency:copy -Dartifact=org.apache.commons:commons-csv:1.14.1 -DoutputDirectory="$MI_HOME/lib"
   ```

   Do **not** add commons-vfs, smbj or asn-one: MI 4.1.0 already provides them, and a second copy breaks class loading.

2. **Remove stale bundles** when upgrading: delete any `<MI_HOME>/dropins/mi_inbound_streaming_*.jar`, so MI regenerates the OSGi bundle from the new jar.
3. **Restart MI.** Jars in `lib` are only picked up at startup. Inbound endpoint and sequence XML files are hot-deployed as usual.
4. **CSV into the message body only:** register a plain-text builder for `text/csv` in `<MI_HOME>/conf/deployment.toml`, then restart. Without it, MI tries to parse each CSV record as XML. This is not needed when the output goes to a property.

   ```toml
   [[custom_message_builders]]
   content_type = "text/csv"
   class = "org.apache.axis2.format.PlainTextBuilder"
   ```

## Quick start

Stream a large CSV in chunks of 2,000 rows into a property. Each chunk is mediated, and its summary is written to a JSON Lines reply file.

`<MI_HOME>/repository/deployment/server/synapse-configs/default/inbound-endpoints/ordersInbound.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<inboundEndpoint xmlns="http://ws.apache.org/ns/synapse" name="ordersInbound"
                 class="org.wso2.carbon.inbound.streaming.VFSConsumer"
                 sequence="orderChunkSeq" onError="fault" suspend="false">
    <parameters>
        <parameter name="scheduleType">Polling</parameter>
        <parameter name="interval">5000</parameter>
        <parameter name="sequential">true</parameter>
        <parameter name="coordination">true</parameter>

        <parameter name="transport.vfs.FileURI">/data/in</parameter>
        <parameter name="transport.vfs.FileNamePattern">.*\.csv</parameter>
        <parameter name="transport.vfs.FileProcessCount">1</parameter>
        <parameter name="transport.vfs.Locking">enable</parameter>
        <parameter name="transport.vfs.ActionAfterProcess">MOVE</parameter>
        <parameter name="transport.vfs.MoveAfterProcess">/data/out</parameter>
        <parameter name="transport.vfs.ActionAfterFailure">MOVE</parameter>
        <parameter name="transport.vfs.MoveAfterFailure">/data/fault</parameter>

        <parameter name="transport.vfs.Streaming">true</parameter>
        <parameter name="transport.vfs.StreamingMode">CHUNK</parameter>
        <parameter name="transport.vfs.StreamingInputFormat">csv</parameter>
        <parameter name="transport.vfs.StreamingChunkSize">2000</parameter>
        <parameter name="transport.vfs.StreamingBufferSize">65536</parameter>
        <parameter name="transport.vfs.StreamingCsvHasHeader">true</parameter>
        <parameter name="transport.vfs.StreamingAddHeadersToEachResult">true</parameter>
        <parameter name="transport.vfs.StreamingOutputProperty">streamOut</parameter>
        <parameter name="transport.vfs.StreamingCsvDataTypes">[{"Column Name Or Index":"order_id","Is Column Name":"Yes","Data Type":"Integer"},{"Column Name Or Index":"unit_price","Is Column Name":"Yes","Data Type":"Number"},{"Column Name Or Index":"quantity","Is Column Name":"Yes","Data Type":"Integer"}]</parameter>
        <parameter name="transport.vfs.StreamingParallelism">4</parameter>

        <parameter name="transport.vfs.ReplyFileURI">/data/replies</parameter>
        <parameter name="transport.vfs.ReplyFileName">order-batches.jsonl</parameter>
    </parameters>
</inboundEndpoint>
```

`sequences/orderChunkSeq.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<sequence name="orderChunkSeq" xmlns="http://ws.apache.org/ns/synapse">
    <script language="js"><![CDATA[
        var rows = JSON.parse(mc.getProperty('streamOut'));
        var total = 0;
        for (var i = 0; i < rows.length; i++) {
            total += rows[i].unit_price * rows[i].quantity;
        }
        mc.setPayloadJSON({ chunk: mc.getProperty('STREAMING_CHUNK_NUMBER'),
                            rows: rows.length, revenue: Math.round(total * 100) / 100 });
    ]]></script>
</sequence>
```

Each chunk adds one line to `/data/replies/order-batches.jsonl`:

```json
{"chunk":"1","rows":2000,"revenue":418233.17}
```

> Use the `class` attribute with `scheduleType`, not `protocol="polling"`; MI 4.1.0 rejects the latter. Both `transport.vfs.Streaming=true` **and** a `StreamingMode` are needed. With `Streaming` left at its default (`false`), the whole file is read as one message.

## How a file is processed

1. Poll the folder. Skip files that don't match `FileNamePattern`, are locked, or are still being written (see `CheckSizeInterval`).
2. Take the `.lock` beside the file.
3. Look for a checkpoint. If one matches, resume at the recorded record; otherwise start at record 1.
4. Read the file as a stream, one record or chunk at a time:
   - inject it into the sequence (on worker threads when `StreamingParallelism` > 1);
   - append what the sequence produced to the reply file;
   - send failed records to the error files;
   - every `StreamingCheckpointInterval` units, save the checkpoint.
5. At the end of the file, save the final position, run `StreamingFileCompleteSequence`, move or delete the file, then clear the checkpoint.

A graceful shutdown stops at a record boundary. It finishes the units in flight, saves a checkpoint, releases the lock and leaves the file in place, so it resumes on the next start. Neither completion sequence runs: the file is paused, not finished.

## Configuration reference

All parameters go inside `<parameters>` of the inbound endpoint. Defaults are what the inbound uses when the parameter is absent. The MI tooling form may pre-fill a different value, as noted.

### Inbound basics

| Parameter | Default | Description |
|---|---|---|
| `class` | — | Must be `org.wso2.carbon.inbound.streaming.VFSConsumer`. |
| `sequence` / `onError` | — | The sequence each record or chunk is injected into, and its fault sequence. |
| `scheduleType` | — | `Polling`. |
| `interval` | — | Milliseconds between polls. |
| `sequential` | `true` | Run polls one at a time. |
| `coordination` | `true` | In a cluster, only the coordinator node polls. Keep it on for shared folders. |

### Source and file selection

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.FileURI` | — (required) | Folder or file to poll: a local path or a VFS URI (`file://`, `ftp://`, `ftps://`, `sftp://`, `smb2://`). Credentials and VFS options go in the URI, e.g. `sftp://user:pass@host/in?vfs.passive=true`. Passwords are masked in logs. |
| `transport.vfs.FileNamePattern` | all files | Java regex a file name must match, e.g. `.*\.csv`. |
| `transport.vfs.FileProcessCount` | all | Maximum files consumed per poll. Use `1` for very large files. |
| `transport.vfs.FileProcessInterval` | `0` | Milliseconds to pause between files within one poll. |
| `transport.vfs.FileSortAttribute` | none | Sort files by `Name`, `Size` or `Lastmodifiedtimestamp`. |
| `transport.vfs.FileSortAscending` | `true` | Sort direction. |
| `transport.vfs.MinimumAge` / `MaximumAge` | none | Only process files older / newer than this many milliseconds. |
| `transport.vfs.FileSizeLimit` | `-1` | Skip files larger than this many bytes (`-1` = no limit). |
| `transport.vfs.CheckSizeInterval` | `1000` | Read a file's size and last-modified time, wait this many milliseconds and read them again. The file is processed only if neither changed. Only metadata is read, never the content. `0` disables the check (use only if producers write atomically). |
| `transport.vfs.CheckSizeIgnoreEmpty` | `false` | Also hold back empty (0-byte) files. |
| `transport.vfs.ContentType` | none | Content type for `ENTIRE_FILE` mode. Ignored when streaming, where the input format decides it. |
| `transport.vfs.Build` | `false` | Build the message fully before injection. |
| `transport.vfs.ResolveHostsDynamically` | `false` | Re-resolve the host name on each connection. |

### Streaming

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.Streaming` | `false` | Must be `true` for `CHUNK` and `RECORD` modes. |
| `transport.vfs.StreamingMode` | `ENTIRE_FILE` | `RECORD`: one message per record. `CHUNK`: one message per `StreamingChunkSize` records. `ENTIRE_FILE`: one message for the file (no streaming, no checkpointing). |
| `transport.vfs.StreamingInputFormat` | `text` | `csv`, `json`, `jsonl` or `text` (one line per record). |
| `transport.vfs.StreamingChunkSize` | `1` | Records per message in `CHUNK` mode. 1,000–5,000 is typical for large files. |
| `transport.vfs.StreamingParallelism` | `1` | Records (`RECORD`) or chunks (`CHUNK`) mediated concurrently. See [Parallel mediation](#parallel-mediation). |
| `transport.vfs.StreamingBufferSize` | `8192` | Read buffer in bytes. `65536` helps on multi-gigabyte files. |
| `transport.vfs.StreamingCharset` | `UTF-8` | Charset used to decode the file. |
| `transport.vfs.StreamingJsonPath` | `$[*]` | `json` only: selects the repeating element, e.g. `$.orders[*]`. |
| `transport.vfs.StreamingOutputProperty` | unset | Put the parsed output in this message property, as a JSON string: an object in `RECORD` mode, an array in `CHUNK` mode. Read it with `get-property('<name>')`. When unset, the output becomes the message body. The legacy name `transport.vfs.StreamingOutputVariable` is accepted as an alias. |

### CSV

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.StreamingCsvHasHeader` | `true` | The first line holds the column names and is not delivered as a record. |
| `transport.vfs.StreamingCsvDelimiter` | `,` | Field separator (first character used). |
| `transport.vfs.StreamingCsvQuote` | `"` | Quote character. |
| `transport.vfs.StreamingAddHeadersToEachResult` | `false` | `true`: each row is an object keyed by header (`{"id":1,"name":"Alice"}`). `false`: each row is an array (`["1","Alice"]`). |
| `transport.vfs.StreamingCsvDataTypes` | all strings | JSON array declaring column types; see below. Alias: `transport.vfs.csvDataTypes`. |

CSV values are strings unless a type is declared:

```json
[
  {"Column Name Or Index": "order_id",  "Is Column Name": "Yes", "Data Type": "Integer"},
  {"Column Name Or Index": "unit_price","Is Column Name": "Yes", "Data Type": "Number"},
  {"Column Name Or Index": "2",         "Is Column Name": "No",  "Data Type": "Boolean"}
]
```

- **Column reference:** `Is Column Name: Yes` refers to a header name; `No` refers to a zero-based column index.
- **Types:**
  - `Integer` is a 64-bit integer.
  - `Number` keeps the value exactly as written: `120.50` stays `120.50`.
  - `Boolean` accepts only `true` or `false`.
  - `String` is the default.
- **Values that won't convert** are kept as strings, and the column is warned about once per file. Nothing is dropped and nothing fails.

### Reply (output) file

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.ReplyFileURI` | unset | Folder to write the mediated output to. When unset, nothing is written. |
| `transport.vfs.ReplyFileName` | `response.jsonl` / `response.xml` | Output file name. By default JSON results go to `response.jsonl` (one JSON document per line) and anything else to `response.xml`. |

The sequence's final message is appended for every record or chunk that was mediated successfully, in file order. An empty body is skipped. Keep `payloadFactory` formats on one line when writing JSON Lines.

### Per-record error handling

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.StreamingParseErrorAction` | `MOVE` | For records that fail to parse: `MOVE` writes them to an error file, `DROP` discards them. |
| `transport.vfs.StreamingParseErrorFolder` | `MoveAfterFailure` | Folder for parse-error files. |
| `transport.vfs.StreamingMediationErrorAction` | `MOVE` | For records or chunks whose mediation failed: `MOVE` or `DROP`. |
| `transport.vfs.StreamingMediationErrorFolder` | parse-error folder, then `MoveAfterFailure` | Folder for mediation-error files. |

### Checkpointing and read resilience

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.StreamingCheckpointEnabled` | `true` | Save resume points to the MI registry (`CHUNK` and `RECORD` modes). |
| `transport.vfs.StreamingCheckpointInterval` | `1000` | Units between saves: chunks in `CHUNK` mode, records in `RECORD` mode. With chunk size 2,000, `50` saves a resume point every 100,000 rows. |
| `transport.vfs.StreamingReadRetries` | `3` | Consecutive read failures tolerated when the connection drops mid-file. The file is reopened and reading continues at the exact byte where it stopped. `0` disables this, so the first failure fails the file. |
| `transport.vfs.StreamingReadRetryDelay` | `5000` | Milliseconds before a read retry, multiplied by the failure count (5 s, 10 s, 15 s …). |

### Completion callbacks

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.StreamingFileCompleteSequence` | unset | Sequence invoked once after a file is fully processed. |
| `transport.vfs.StreamingFileFailureSequence` | unset | Sequence invoked once when a file fails as a whole. |

Both receive a JSON summary as the body, plus the same values as properties (see [below](#message-context-properties)):

```json
{"file":"orders.csv","uri":"file:///data/in/orders.csv","status":"COMPLETED","processed":8663312,"parseFailed":0,"mediationFailed":0}
```

A failing callback is logged, but it never changes the file's outcome.

### After processing

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.ActionAfterProcess` | `DELETE` (tooling pre-fills `MOVE`) | `MOVE`, `DELETE` or `NONE` on success. |
| `transport.vfs.MoveAfterProcess` | — | Destination folder when the action is `MOVE`. |
| `transport.vfs.ActionAfterFailure` | `DELETE` (tooling pre-fills `MOVE`) | `MOVE`, `DELETE` or `NONE` when a file fails as a whole. |
| `transport.vfs.MoveAfterFailure` | — | Destination folder for failed files. |
| `transport.vfs.SubFolderTimestampFormat` | none | Date pattern for a dated sub-folder under the destination. |
| `transport.vfs.CreateFolder` | `false` | Create missing destination folders. |
| `transport.vfs.UpdateLastModified` | `false` (tooling pre-fills `true`) | `true` sets a moved file's last-modified time to the time of the move; `false` keeps the original. |
| `transport.vfs.MoveAfterFailedMove` | none | Where to put a file whose move itself failed. |
| `transport.vfs.FailedRecordsFileName` | `vfs-move-failed-records.properties` | Record of files whose move failed, retried on later polls. |
| `transport.vfs.FailedRecordsFileDestination` | `repository/conf/` | Folder for that record file. |
| `transport.vfs.MoveFailedRecordTimestampFormat` | `dd-MM-yyyy HH:mm:ss` | Timestamp format in that record file. |
| `transport.vfs.FailedRecordNextRetryDuration` | `3000` | Milliseconds before a failed move is retried. |

### Locking and connections

| Parameter | Default | Description |
|---|---|---|
| `transport.vfs.Locking` | `disable` (tooling pre-fills `enable`) | `enable` takes a `.lock` file beside the source so that only one node processes it. Enable it for shared or clustered sources. |
| `transport.vfs.AutoLockRelease` | `false` | Reclaim a `.lock` left behind by a crashed node. |
| `transport.vfs.AutoLockReleaseInterval` | any age | Minimum lock age, in milliseconds, before it may be reclaimed. |
| `transport.vfs.LockReleaseSameNode` | `false` | `true` only reclaims locks written by this host. |
| `transport.vfs.MaxRetryCount` | `3` | Connection attempts before a poll gives up. |
| `transport.vfs.ReconnectTimeout` | `30000` | Milliseconds between connection attempts. |
| `transport.vfs.SFTPTimeout` | none | SFTP socket read timeout in milliseconds. Set it for any SFTP server you don't control, but keep it above the slowest gap between two reads (which depends on how fast your sequence mediates). |
| `transport.vfs.SFTPConnectTimeout` | `30000` | SFTP TCP connect timeout in milliseconds. Requires commons-vfs `2.2.0-wso2v13.20` or later; on older update levels `SFTPTimeout` bounds the connect instead. |
| `transport.vfs.SFTPIdentities` | none | Private key file(s) for key-based SFTP authentication. |
| `transport.vfs.SFTPIdentityPassPhrase` | none | Passphrase for those keys. |
| `transport.vfs.SFTPUserDirIsRoot` | VFS default (`true`) | Resolve paths relative to the login directory. |

The `SFTP*` options can also be given as query parameters on `FileURI`, without the `SFTP` prefix, e.g. `?Timeout=45000`.

## Message context properties

Set on every streamed message, whether the output went to a property or to the body:

| Property | Mode | Value |
|---|---|---|
| `STREAMING_RECORD_NUMBER` | `RECORD` | Absolute, 1-based record number in the file. |
| `STREAMING_CHUNK_NUMBER` | `CHUNK` | Absolute, 1-based chunk number. It keeps counting across a resume. |
| `STREAMING_FIRST_RECORD` / `STREAMING_LAST_RECORD` | `CHUNK` | Record range in this message. |
| `STREAMING_CHUNK_SIZE` | `CHUNK` | Records actually in this message (smaller for the final chunk). |

Set on the completion callback message:

| Property | Value |
|---|---|
| `STREAMING_FILE_NAME` | Source file name. |
| `STREAMING_FILE_URI` | Source URI, with any password masked. |
| `STREAMING_STATUS` | `COMPLETED` or `FAILED`. |
| `STREAMING_PROCESSED` | Records delivered to the sequence. |
| `STREAMING_PARSE_FAILED` | Records that failed to parse. |
| `STREAMING_MEDIATION_FAILED` | Records whose mediation failed. |

## Parallel mediation

With `transport.vfs.StreamingParallelism` set to **N > 1**, a file is processed by a pipeline:

```
polling thread                 worker threads (N)              polling thread
read + parse next unit  ──►    mediate units concurrently ──►  commit, oldest first:
(up to 2N units ahead)                                           reply file, error files,
                                                                 counters, checkpoint
```

- **Order is preserved.** Results are committed in file order, so the reply file, error files and checkpoint look the same as a serial run. The completion sequence runs only after every unit has finished.
- **Memory is bounded:** at most 2N units are held at once (2N × `StreamingChunkSize` rows in `CHUNK` mode).
- **The default (`1`) is serial:** every unit is mediated on the polling thread, one at a time.
- **The sequence must finish within the injection,** because the inbound reads each unit's success or failure as soon as mediation returns. Use `<call blocking="true">` (or a callout), not a non-blocking `<call>`.

**Sizing N:**

| Sequence type | Suggested value |
|---|---|
| I/O-bound: calls a backend, database or API per record or chunk | Higher than the CPU core count, up to what the backend can take. The speedup is close to N. |
| CPU-bound: scripts and transformations only | About the number of physical cores − 1 (the polling thread needs one). On a coordinated cluster node, leave more headroom, so the node's cluster heartbeat is not starved. |

Large chunks with high parallelism need heap: give MI enough memory for 2N chunks in flight.

**After a crash:** a hard kill can repeat up to 2N in-flight units plus the units since the last checkpoint, so the sequence (or its backend) should tolerate re-processing. A graceful shutdown finishes the in-flight units first, so it repeats nothing.

## Checkpointing and resume

- **Storage:** checkpoints are kept in the MI registry at `gov:/fileStreamingCheckpoints/<inbound name>/<CRC32 of the file URI>.json`, with a `.bak` copy of the previous write. No database is needed. The file URI itself is never stored, because it may contain credentials.
- **Matching:** a checkpoint is used only if the file still matches it, by size and a CRC32 of its first 64 KB. The modification time is ignored, so moving a file away and copying it back still resumes.
- **Configuration changes:** a configuration hash covers the format, mode, chunk size, charset, CSV settings, JSON path, output property and column types. Change any of these and the file starts fresh instead of skipping the wrong records.
- **Logging:** when a checkpoint is found but not used, the reason is logged (`Ignoring the streaming checkpoint … because …`).
- **Clusters:** for one node to resume what another started, both must read the same governance registry. Point every node's `GovRegRoot` (in `registry.xml`) at a shared location. Don't symlink the registry folder.

## Error handling

- **One bad record never fails the file.**
  - Parse failures and mediation failures are counted, and moved to error files or dropped as configured.
  - The error files sit next to each other in the configured folder, named after the source: `orders.csv` gives `orders.parse.fail.csv` and `orders.mediation.fail.csv`.
  - In `CHUNK` mode a chunk whose mediation fails is written out whole, in the shape it was sent.
- **Whole-file failures:** a structural or I/O error fails the file (for example a CSV that becomes unparseable, or a connection that cannot be re-established). `ActionAfterFailure` / `MoveAfterFailure` then apply, and `StreamingFileFailureSequence` runs.
- **Summary line:** every file ends with a log line giving its totals:

  ```
  Streaming completed for orders.csv: processed=8663312, parseFailed=0, mediationFailed=0
  ```

## Samples

**Stream a large JSON array record by record**, selecting the repeating element:

```xml
<parameter name="transport.vfs.Streaming">true</parameter>
<parameter name="transport.vfs.StreamingMode">RECORD</parameter>
<parameter name="transport.vfs.StreamingInputFormat">json</parameter>
<parameter name="transport.vfs.StreamingJsonPath">$.products[*]</parameter>
<parameter name="transport.vfs.StreamingOutputProperty">item</parameter>
```

**CSV row by row into the message body**, with an XML reply file (needs the `text/csv` builder from [Installation](#installation)):

```xml
<parameter name="transport.vfs.Streaming">true</parameter>
<parameter name="transport.vfs.StreamingMode">RECORD</parameter>
<parameter name="transport.vfs.StreamingInputFormat">csv</parameter>
<parameter name="transport.vfs.StreamingAddHeadersToEachResult">true</parameter>
<parameter name="transport.vfs.ReplyFileURI">/data/replies</parameter>
<parameter name="transport.vfs.ReplyFileName">customers.xml</parameter>
```

**Clustered, with resume and completion callbacks:**

```xml
<parameter name="coordination">true</parameter>
<parameter name="transport.vfs.Locking">enable</parameter>
<parameter name="transport.vfs.AutoLockRelease">true</parameter>
<parameter name="transport.vfs.AutoLockReleaseInterval">30000</parameter>
<parameter name="transport.vfs.StreamingCheckpointEnabled">true</parameter>
<parameter name="transport.vfs.StreamingCheckpointInterval">50</parameter>
<parameter name="transport.vfs.StreamingFileCompleteSequence">orderCompleteSeq</parameter>
<parameter name="transport.vfs.StreamingFileFailureSequence">orderFailedSeq</parameter>
```

More complete configurations, and a sample for a failure sequence, are in [`docs/`](docs) and [`demo/`](demo).

## Limitations on MI 4.1.0

- **Polling only:** no `cronExpression` support on this runtime; use `interval`. Use the `class` attribute, not `protocol="polling"`.
- **Properties, not variables:** MI 4.1.0 has no message variables, so streaming output is delivered in a property.
- **Formats:** XML input is not supported for streaming; use `csv`, `json`, `jsonl` or `text`.
- **`ENTIRE_FILE` mode** reads the file as a single message, with no checkpointing or parallelism.

## Building from source

Requirements: **JDK 8** (or newer, compiling to Java 8) and **Maven 3.6+**.

```bash
mvn clean install
```

This produces:
- `target/mi-inbound-streaming-<version>.jar`, the inbound endpoint;
- `target/mi-inbound-streaming-<version>.zip`, the connector-store bundle (jar, `uischema.json`, `connector.xml`, `descriptor.yml`).

> **Dependency access:** the build compiles against MI 4.1.0 *update-level* artifacts, which are not on the public WSO2 Nexus. You need access to the WSO2 support Maven repository (`https://support-maven.wso2.org/nexus/content/repositories/updates-2.0/`) in your `~/.m2/settings.xml`. The artifacts are:
> - `org.wso2.ei:org.wso2.micro.integrator.inbound.endpoint:4.1.0.142`
> - `org.wso2.org.apache.commons:commons-vfs2:2.2.0-wso2v13.20`
> - `org.wso2.org.apache.commons:commons-vfs2-sandbox:2.2.0-wso2v13.20`

This inbound is a port of the upstream [`mi-inbound-file`](https://github.com/wso2-extensions/mi-inbound-file) streaming work to MI 4.1.0.

## License

Licensed under the [Apache License 2.0](LICENSE).
