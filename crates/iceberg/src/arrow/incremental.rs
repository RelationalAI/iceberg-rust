// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use futures::channel::mpsc::channel;
use futures::stream::select;
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;

use crate::arrow::reader::process_record_batch_stream;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::{ArrowReader, StreamsInto};
use crate::delete_vector::DeleteVector;
use crate::expr::Bind;
use crate::io::FileIO;
use crate::metadata_columns::{RESERVED_FIELD_ID_POS, row_pos_field};
use crate::runtime::spawn;
use crate::scan::ArrowRecordBatchStream;
use crate::scan::incremental::{
    AppendedFileScanTask, DeleteScanTask, EqualityDeleteScanTask, IncrementalFileScanTaskStreams,
};
use crate::spec::{Datum, PrimitiveType};
use crate::{Error, ErrorKind, Result};

/// Default batch size for incremental delete operations.
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Returns `ArrowReaderOptions` with the `_pos` virtual column enabled.
fn pos_arrow_reader_options() -> Result<ArrowReaderOptions> {
    Ok(ArrowReaderOptions::new().with_virtual_columns(vec![Arc::clone(row_pos_field())])?)
}

/// The type of incremental batch: appended data or deleted records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalBatchType {
    /// Appended records.
    Append,
    /// Deleted records.
    Delete,
}

/// The stream of incremental Arrow `RecordBatch`es with batch type.
pub type CombinedIncrementalBatchRecordStream =
    Pin<Box<dyn Stream<Item = Result<(IncrementalBatchType, RecordBatch)>> + Send + 'static>>;

/// Stream type for obtaining a separate stream of appended and deleted record batches.
pub type UnzippedIncrementalBatchRecordStream = (ArrowRecordBatchStream, ArrowRecordBatchStream);

async fn process_incremental_append_task(
    task: AppendedFileScanTask,
    batch_size: Option<usize>,
    file_io: FileIO,
    metadata_size_hint: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    // Check if _pos column is requested and add it as a virtual column
    let has_pos_column = task.base.project_field_ids.contains(&RESERVED_FIELD_ID_POS);
    let virtual_columns = if has_pos_column {
        vec![Arc::clone(row_pos_field())]
    } else {
        vec![]
    };

    // Open the file, then resolve the schema for migrated tables lacking embedded field IDs.
    let initial_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        &task.base.data_file_path,
        file_io.clone(),
        false,
        Some(
            parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                .with_virtual_columns(virtual_columns.clone())?,
        ),
        metadata_size_hint,
        task.base.file_size_in_bytes,
    )
    .await?;

    let (mut record_batch_stream_builder, missing_field_ids) = ArrowReader::resolve_parquet_schema(
        initial_builder,
        &task.base.data_file_path,
        file_io,
        false,
        virtual_columns,
        metadata_size_hint,
        task.base.file_size_in_bytes,
        None, // incremental scan does not yet carry name_mapping
    )
    .await?;

    // Create a projection mask for the batch stream to select which columns in the
    // Parquet file that we want in the response
    let projection_mask = ArrowReader::get_arrow_projection_mask(
        &task.base.project_field_ids,
        &task.schema_ref(),
        record_batch_stream_builder.parquet_schema(),
        record_batch_stream_builder.schema(),
        missing_field_ids,
    )?;
    record_batch_stream_builder = record_batch_stream_builder.with_projection(projection_mask);

    // RecordBatchTransformer performs any transformations required on the RecordBatches
    // that come back from the file, such as type promotion, default column insertion,
    // column re-ordering, and virtual field addition (like _file)
    let datum = Datum::new(
        PrimitiveType::String,
        crate::spec::PrimitiveLiteral::String(task.base.data_file_path.clone()),
    );
    let mut record_batch_transformer_builder =
        RecordBatchTransformerBuilder::new(task.schema_ref(), &task.base.project_field_ids)
            .with_constant(crate::metadata_columns::RESERVED_FIELD_ID_FILE, datum);

    if has_pos_column {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_virtual_field(Arc::clone(row_pos_field()))?;
    }

    let mut record_batch_transformer = record_batch_transformer_builder.build();

    if let Some(batch_size) = batch_size {
        record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
    }

    // Apply positional deletes as row selections.
    let row_selection = if let Some(ref positional_delete_indexes) = task.positional_deletes {
        Some(ArrowReader::build_deletes_row_selection(
            record_batch_stream_builder.metadata().row_groups(),
            &None,
            &positional_delete_indexes.lock().unwrap(),
        )?)
    } else {
        None
    };

    if let Some(row_selection) = row_selection {
        record_batch_stream_builder = record_batch_stream_builder.with_row_selection(row_selection);
    }

    // Apply equality deletes as a row filter predicate.
    if let Some(ref combined_predicate) = task.equality_delete_predicate {
        let schema_ref = task.schema_ref();
        let bound_predicate = combined_predicate.bind(schema_ref, false)?;
        record_batch_stream_builder = ArrowReader::configure_predicate_filtering(
            record_batch_stream_builder,
            &bound_predicate,
            &task.schema_ref(),
            false, // no row-group pruning for append predicate
        )?;
    }

    // Build the batch stream and send all the RecordBatches that it generates
    // to the requester.
    let record_batch_stream = record_batch_stream_builder
        .build()?
        .map(move |batch| match batch {
            Ok(batch) => record_batch_transformer.process_record_batch(batch),
            Err(err) => Err(err.into()),
        });

    Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
}

/// Helper function to create a RecordBatch from a chunk of position values.
/// Creates a batch with file_path column first, then pos column (Int64).
fn create_delete_batch(
    schema: &Arc<ArrowSchema>,
    file_path: &str,
    chunk: Vec<u64>,
) -> Result<RecordBatch> {
    let num_rows = chunk.len();

    // Create file path array (repeated for each row)
    let file_array = arrow_array::StringArray::from(vec![file_path; num_rows]);

    // Create Int64 array for positions
    let pos_array = Int64Array::from_iter(chunk.iter().map(|&i| Some(i as i64)));

    RecordBatch::try_new(Arc::clone(schema), vec![
        Arc::new(file_array),
        Arc::new(pos_array),
    ])
    .map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to create RecordBatch for delete positions",
        )
    })
}

fn process_incremental_delete_task(
    file_path: String,
    delete_vector: DeleteVector,
    batch_size: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    // Create schema with file_path column first, then pos (Int64)
    let schema = Arc::new(ArrowSchema::new(vec![
        Arc::clone(crate::metadata_columns::file_path_field()),
        Arc::clone(crate::metadata_columns::pos_field_arrow()),
    ]));

    let treemap = delete_vector.inner;

    let stream = futures::stream::iter(treemap)
        .chunks(batch_size)
        .map(move |chunk| create_delete_batch(&schema, &file_path, chunk));

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}

fn process_incremental_deleted_file_task(
    file_path: String,
    total_records: u64,
    batch_size: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    // Create schema with file_path column first, then pos (Int64)
    let schema = Arc::new(ArrowSchema::new(vec![
        Arc::clone(crate::metadata_columns::file_path_field()),
        Arc::clone(crate::metadata_columns::pos_field_arrow()),
    ]));

    // Create a stream of position values from 0 to total_records-1 (0-indexed)
    let stream = futures::stream::iter(0..total_records)
        .chunks(batch_size)
        .map(move |chunk| create_delete_batch(&schema, &file_path, chunk));

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}

/// Process equality delete task by reading the data file with equality delete predicates applied
/// as a row filter, and emitting record batches containing matching row positions.
async fn process_equality_delete_task(
    task: EqualityDeleteScanTask,
    batch_size: Option<usize>,
    file_io: crate::io::FileIO,
) -> Result<ArrowRecordBatchStream> {
    let file_path = task.data_file_path().to_string();

    // Create output schema with file_path column first, then pos (Int64)
    let output_schema = Arc::new(ArrowSchema::new(vec![
        Arc::clone(crate::metadata_columns::file_path_field()),
        Arc::clone(crate::metadata_columns::pos_field_arrow()),
    ]));

    // Open the Parquet file with page index loaded (needed for row group filtering).
    // We always have a predicate for equality deletes, so page index is always useful.
    let virtual_columns = vec![Arc::clone(row_pos_field())];
    let initial_stream_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        &file_path,
        file_io.clone(),
        true,
        Some(pos_arrow_reader_options()?),
        None,
        task.base.file_size_in_bytes,
    )
    .await?;

    // Schema resolution: detect if Parquet file lacks embedded field IDs (migrated tables)
    // and assign fallback IDs before reading.
    let (mut record_batch_stream_builder, missing_field_ids) = ArrowReader::resolve_parquet_schema(
        initial_stream_builder,
        &file_path,
        file_io,
        true,
        virtual_columns,
        None,
        task.base.file_size_in_bytes,
        None, // equality delete tasks do not carry name_mapping
    )
    .await?;

    // The combined_predicate selects rows TO DELETE. Bind it to the task schema.
    let bound_predicate = task.combined_predicate.bind(task.schema_ref(), false)?;

    // Column projection: only read the columns referenced by the predicate.
    // The _pos virtual column is handled separately and does not need projection.
    let (iceberg_field_ids, _) = ArrowReader::build_field_id_set_and_map(
        record_batch_stream_builder.parquet_schema(),
        &bound_predicate,
    )?;
    let predicate_field_ids: Vec<i32> = iceberg_field_ids.iter().copied().collect();
    let projection_mask = ArrowReader::get_arrow_projection_mask(
        &predicate_field_ids,
        &task.schema_ref(),
        record_batch_stream_builder.parquet_schema(),
        record_batch_stream_builder.schema(),
        missing_field_ids,
    )?;
    record_batch_stream_builder = record_batch_stream_builder.with_projection(projection_mask);

    // Row filter + row group filtering: apply the equality delete predicate.
    // Row group pruning is safe here because combined_predicate has been rewritten
    // via rewrite_not() so RowGroupMetricsEvaluator can evaluate it correctly.
    record_batch_stream_builder = ArrowReader::configure_predicate_filtering(
        record_batch_stream_builder,
        &bound_predicate,
        &task.base.schema,
        true, // row group filtering enabled
    )?;

    if let Some(batch_size) = batch_size {
        record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
    }

    // Build the stream of filtered records
    let record_batch_stream = record_batch_stream_builder.build()?;

    // Extract positions from the _pos column and emit delete batches
    let output_schema_clone = output_schema.clone();
    let file_path_clone = file_path.clone();

    let stream = record_batch_stream
        .then(move |batch_result| {
            let schema = output_schema_clone.clone();
            let path = file_path_clone.clone();
            async move {
                match batch_result {
                    Ok(batch) => {
                        // Extract _pos column (last column due to virtual_columns).
                        // _pos is always non-null: it is a virtual column representing the
                        // physical row position, produced by the Parquet reader for every row.
                        let pos_col = batch
                            .column(batch.num_columns() - 1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                Error::new(
                                    ErrorKind::Unexpected,
                                    "Failed to extract _pos column from equality delete batch",
                                )
                            })?;

                        let num_rows = pos_col.len();
                        if num_rows == 0 {
                            return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
                        }

                        // Reuse the Int64Array directly as the pos column.
                        // Build a matching file_path StringArray.
                        let file_array = Arc::new(arrow_array::StringArray::from(vec![
                            path.as_str(
                            );
                            num_rows
                        ]));
                        RecordBatch::try_new(Arc::clone(&schema), vec![
                            file_array,
                            Arc::clone(batch.column(batch.num_columns() - 1)),
                        ])
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::Unexpected,
                                format!("Failed to create equality delete batch: {e}"),
                            )
                        })
                    }
                    Err(e) => Err(e.into()),
                }
            }
        })
        .boxed();

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}

impl StreamsInto<ArrowReader, CombinedIncrementalBatchRecordStream>
    for IncrementalFileScanTaskStreams
{
    /// Takes separate streams of appended and deleted file scan tasks and reads all the files.
    /// Returns a combined stream of Arrow `RecordBatch`es containing the data from the files.
    fn stream(self, reader: ArrowReader) -> Result<CombinedIncrementalBatchRecordStream> {
        let (appends, deletes) =
            StreamsInto::<ArrowReader, UnzippedIncrementalBatchRecordStream>::stream(self, reader)?;

        let left = appends.map(|res| res.map(|batch| (IncrementalBatchType::Append, batch)));
        let right = deletes.map(|res| res.map(|batch| (IncrementalBatchType::Delete, batch)));

        Ok(Box::pin(select(left, right)) as CombinedIncrementalBatchRecordStream)
    }
}

impl StreamsInto<ArrowReader, UnzippedIncrementalBatchRecordStream>
    for IncrementalFileScanTaskStreams
{
    /// Takes separate streams of appended and deleted file scan tasks and reads all the files.
    /// Returns two separate streams of Arrow `RecordBatch`es containing appended data and deleted records.
    fn stream(self, reader: ArrowReader) -> Result<UnzippedIncrementalBatchRecordStream> {
        let (appends_tx, appends_rx) =
            channel::<Result<RecordBatch>>(reader.concurrency_limit_data_files);
        let (deletes_tx, deletes_rx) =
            channel::<Result<RecordBatch>>(reader.concurrency_limit_data_files);

        let batch_size = reader.batch_size;
        let metadata_size_hint = reader.metadata_size_hint;

        let (append_stream, delete_stream) = self;

        // Process append tasks
        let file_io_append = reader.file_io.clone();
        spawn(async move {
            let _ = append_stream
                .try_for_each_concurrent(reader.concurrency_limit_data_files, |append_task| {
                    let file_io = file_io_append.clone();
                    let appends_tx = appends_tx.clone();
                    async move {
                        spawn(async move {
                            let record_batch_stream = process_incremental_append_task(
                                append_task,
                                batch_size,
                                file_io,
                                metadata_size_hint,
                            )
                            .await;

                            process_record_batch_stream(
                                record_batch_stream,
                                appends_tx,
                                "failed to read appended record batch",
                            )
                            .await;
                        });
                        Ok(())
                    }
                })
                .await;
        });

        // Process delete tasks
        let file_io_delete = reader.file_io.clone();
        spawn(async move {
            let _ = delete_stream
                .try_for_each_concurrent(reader.concurrency_limit_data_files, |delete_task| {
                    let deletes_tx = deletes_tx.clone();
                    let file_io = file_io_delete.clone();
                    async move {
                        match delete_task {
                            DeleteScanTask::DeletedFile(deleted_file_task) => {
                                spawn(async move {
                                    let file_path = deleted_file_task.data_file_path().to_string();
                                    let total_records =
                                        deleted_file_task.base.record_count.unwrap_or(0);

                                    let record_batch_stream = process_incremental_deleted_file_task(
                                        file_path,
                                        total_records,
                                        batch_size,
                                    );

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        deletes_tx,
                                        "failed to read deleted file record batch",
                                    )
                                    .await;
                                });
                            }
                            DeleteScanTask::PositionalDeletes(file_path, delete_vector) => {
                                spawn(async move {
                                    let record_batch_stream = process_incremental_delete_task(
                                        file_path,
                                        delete_vector,
                                        batch_size,
                                    );

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        deletes_tx,
                                        "failed to read deleted record batch",
                                    )
                                    .await;
                                });
                            }
                            DeleteScanTask::EqualityDeletes(equality_delete_task) => {
                                spawn(async move {
                                    let record_batch_stream = process_equality_delete_task(
                                        equality_delete_task,
                                        batch_size,
                                        file_io.clone(),
                                    )
                                    .await;

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        deletes_tx,
                                        "failed to read equality delete record batch",
                                    )
                                    .await;
                                });
                            }
                        }
                        Ok(())
                    }
                })
                .await;
        });

        Ok((
            Box::pin(appends_rx) as ArrowRecordBatchStream,
            Box::pin(deletes_rx) as ArrowRecordBatchStream,
        ))
    }
}
