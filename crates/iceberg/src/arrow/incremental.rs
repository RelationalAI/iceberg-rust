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
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use futures::channel::mpsc::channel;
use futures::stream::select;
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;

use crate::arrow::reader::{ArrowFileReader, process_record_batch_stream};
use crate::arrow::{ArrowReader, StreamsInto};
use crate::delete_vector::DeleteVector;
use crate::expr::{Bind, BoundPredicate};
use crate::io::FileIO;
use crate::metadata_columns::row_pos_field;
use crate::runtime::spawn;
use crate::scan::ArrowRecordBatchStream;
use crate::scan::incremental::{
    AppendedFileScanTask, BaseIncrementalFileScanTask, DeleteScanTask, EqualityDeleteScanTask,
    IncrementalFileScanTaskStreams,
};
use crate::{Error, ErrorKind, Result};

/// Default batch size for incremental delete operations.
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Opens a Parquet file for an incremental scan task, resolves its schema, applies all
/// configured filtering (byte-range, predicate, positional deletes), and returns a builder
/// ready for `.build()`.
///
/// # Parameters
/// - `should_load_page_index`: whether to load the Parquet page index
/// - `virtual_columns`: virtual columns (e.g. `_pos`) to request from the reader
/// - `bound_predicate`: optional predicate for row filtering and row group pruning
/// - `positional_deletes`: optional positional delete vector for row selection
/// - `row_group_filtering_enabled`: enable row group pruning via predicate statistics
/// - `row_selection_enabled`: enable page-level row selection via predicate
/// - `use_predicate_projection`: when `true`, projects to predicate columns only (for
///   equality delete tasks); when `false`, no projection is applied here (append tasks
///   call `apply_projection` separately with the full output field IDs)
///
/// Returns `(builder, has_missing_field_ids)`.
#[allow(clippy::too_many_arguments)]
async fn prepare_parquet_stream_builder(
    base: &BaseIncrementalFileScanTask,
    file_io: FileIO,
    metadata_size_hint: Option<usize>,
    batch_size: Option<usize>,
    should_load_page_index: bool,
    virtual_columns: Vec<Arc<arrow_schema::Field>>,
    bound_predicate: Option<&BoundPredicate>,
    positional_deletes: Option<&Mutex<DeleteVector>>,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    use_predicate_projection: bool,
) -> Result<(ParquetRecordBatchStreamBuilder<ArrowFileReader>, bool)> {
    let arrow_reader_options = if virtual_columns.is_empty() {
        None
    } else {
        Some(ArrowReaderOptions::new().with_virtual_columns(virtual_columns.clone())?)
    };
    let initial_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        &base.data_file_path,
        file_io.clone(),
        should_load_page_index,
        arrow_reader_options,
        metadata_size_hint,
        base.file_size_in_bytes,
    )
    .await?;

    let (mut builder, has_missing_field_ids) = ArrowReader::resolve_parquet_schema(
        initial_builder,
        &base.data_file_path,
        file_io,
        should_load_page_index,
        virtual_columns,
        metadata_size_hint,
        base.file_size_in_bytes,
        None, // name_mapping not yet supported in incremental scan
    )
    .await?;

    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    let mut selected_row_group_indices = None;
    let mut row_selection = None;

    if base.start != 0 || base.length != 0 {
        selected_row_group_indices = Some(ArrowReader::filter_row_groups_by_byte_range(
            builder.metadata(),
            base.start,
            base.length,
        )?);
    }

    if let Some(predicate) = bound_predicate {
        let predicate_projection = use_predicate_projection.then_some(has_missing_field_ids);
        builder = ArrowReader::apply_predicate_row_filtering(
            builder,
            predicate,
            &base.schema,
            &mut selected_row_group_indices,
            &mut row_selection,
            row_group_filtering_enabled,
            row_selection_enabled,
            predicate_projection,
        )?;
    }

    ArrowReader::apply_positional_delete_row_selection(
        builder.metadata().row_groups(),
        &selected_row_group_indices,
        positional_deletes,
        &mut row_selection,
    )?;

    builder = ArrowReader::apply_row_groups_and_selection(
        builder,
        selected_row_group_indices,
        row_selection,
    );

    Ok((builder, has_missing_field_ids))
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
    mut task: AppendedFileScanTask,
    batch_size: Option<usize>,
    file_io: FileIO,
    metadata_size_hint: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let should_load_page_index =
        task.equality_delete_predicate.is_some() || task.positional_deletes.is_some();
    let schema = task.base.schema.clone();
    let case_sensitive = task.base.case_sensitive;
    let equality_delete_bound = task
        .equality_delete_predicate
        .take()
        .map(|p| p.bind(schema, case_sensitive))
        .transpose()?;

    let (builder, has_missing_field_ids) = prepare_parquet_stream_builder(
        &task.base,
        file_io,
        metadata_size_hint,
        batch_size,
        should_load_page_index,
        ArrowReader::build_virtual_columns(&task.base.project_field_ids),
        equality_delete_bound.as_ref(),
        task.positional_deletes.as_deref(),
        true,  // row_group_filtering_enabled
        true,  // row_selection_enabled
        false, // use_predicate_projection: projection applied separately below
    )
    .await?;

    let builder = ArrowReader::apply_projection(
        builder,
        &task.base.project_field_ids,
        &task.schema_ref(),
        has_missing_field_ids,
    )?;

    let mut record_batch_transformer = ArrowReader::build_record_batch_transformer(
        task.schema_ref(),
        &task.base.project_field_ids,
        &task.base.data_file_path,
        task.base.partition_spec.clone(),
        task.base.partition.clone(),
    )?;

    let record_batch_stream = builder.build()?.map(move |batch| match batch {
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
    file_io: FileIO,
    metadata_size_hint: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let file_path = task.data_file_path().to_string();

    // Create output schema with file_path column first, then pos (Int64)
    let output_schema = Arc::new(ArrowSchema::new(vec![
        Arc::clone(crate::metadata_columns::file_path_field()),
        Arc::clone(crate::metadata_columns::pos_field_arrow()),
    ]));

    let bound_predicate = task
        .combined_predicate
        .bind(task.schema_ref(), task.base.case_sensitive)?;

    let (builder, _) = prepare_parquet_stream_builder(
        &task.base,
        file_io,
        metadata_size_hint,
        batch_size,
        true, // always load page index: we always have a predicate
        vec![Arc::clone(row_pos_field())],
        Some(&bound_predicate),
        None,  // no positional deletes for equality delete tasks
        true,  // row_group_filtering_enabled
        false, // row_selection_enabled
        true,  // use_predicate_projection: project to predicate columns only
    )
    .await?;

    // Build the stream of filtered records
    let record_batch_stream = builder.build()?;

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
                                        metadata_size_hint,
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
