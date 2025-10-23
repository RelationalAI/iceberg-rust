use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::{Stream, StreamExt, TryStreamExt};
use roaring::RoaringTreemap;

use crate::arrow::record_batch_transformer::RecordBatchTransformer;
use crate::arrow::{ArrowBatchEmitter, ArrowReader};
use crate::io::FileIO;
use crate::scan::ArrowRecordBatchStream;
use crate::scan::incremental::{
    AppendedFileScanTask, IncrementalFileScanTask, IncrementalFileScanTaskStream,
};
use crate::{Error, ErrorKind, Result};

/// The type of incremental batch: appended data or deleted records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalBatchType {
    /// Appended records.
    Append,
    /// Deleted records.
    Delete,
}

/// The stream of incremental Arrow `RecordBatch`es with batch type.
pub type IncrementalArrowBatchRecordStream =
    Pin<Box<dyn Stream<Item = Result<(IncrementalBatchType, RecordBatch)>> + Send + 'static>>;

impl ArrowBatchEmitter<ArrowReader, (IncrementalBatchType, RecordBatch)>
    for IncrementalFileScanTaskStream
{
    /// Take a stream of `IncrementalFileScanTasks` and reads all the files. Returns a
    /// stream of Arrow `RecordBatch`es containing the data from the files.
    fn read(self, reader: ArrowReader) -> Result<IncrementalArrowBatchRecordStream> {
        let file_io = reader.file_io.clone();
        let batch_size = reader.batch_size;
        let concurrency_limit_data_files = reader.concurrency_limit_data_files;

        let stream = self
            .map_ok(move |task| {
                let file_io = file_io.clone();

                process_incremental_file_scan_task(task, batch_size, file_io)
            })
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "file scan task generate failed").with_source(err)
            })
            .try_buffer_unordered(concurrency_limit_data_files)
            .try_flatten_unordered(concurrency_limit_data_files);

        Ok(Box::pin(stream) as IncrementalArrowBatchRecordStream)
    }
}

async fn process_incremental_file_scan_task(
    task: IncrementalFileScanTask,
    batch_size: Option<usize>,
    file_io: FileIO,
) -> Result<IncrementalArrowBatchRecordStream> {
    match task {
        IncrementalFileScanTask::Append(append_task) => {
            process_incremental_append_task(append_task, batch_size, file_io)
                .await
                .map(|stream| {
                    // Map the stream to include the batch type
                    let typed_stream = stream.map(|batch_result| {
                        batch_result.map(|batch| (IncrementalBatchType::Append, batch))
                    });
                    Box::pin(typed_stream) as IncrementalArrowBatchRecordStream
                })
        }
        IncrementalFileScanTask::Delete(file_path, delete_vector) => {
            // Clone the `RoaringTreemap` underlying the delete vector to take ownership.
            let bit_map = {
                let guard = delete_vector.lock().unwrap();
                guard.inner.clone()
            };
            process_incremental_delete_task(file_path, bit_map, batch_size).map(|stream| {
                // Map the stream to include the batch type
                let typed_stream = stream.map(|batch_result| {
                    batch_result.map(|batch| (IncrementalBatchType::Delete, batch))
                });
                Box::pin(typed_stream) as IncrementalArrowBatchRecordStream
            })
        }
    }
}

async fn process_incremental_append_task(
    task: AppendedFileScanTask,
    batch_size: Option<usize>,
    file_io: FileIO,
) -> Result<ArrowRecordBatchStream> {
    let mut record_batch_stream_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        &task.data_file_path,
        file_io.clone(),
        true,
    )
    .await?;

    // Create a projection mask for the batch stream to select which columns in the
    // Parquet file that we want in the response
    let projection_mask = ArrowReader::get_arrow_projection_mask(
        &task.project_field_ids,
        &task.schema_ref(),
        record_batch_stream_builder.parquet_schema(),
        record_batch_stream_builder.schema(),
    )?;
    record_batch_stream_builder = record_batch_stream_builder.with_projection(projection_mask);

    // RecordBatchTransformer performs any transformations required on the RecordBatches
    // that come back from the file, such as type promotion, default column insertion
    // and column re-ordering
    let mut record_batch_transformer =
        RecordBatchTransformer::build(task.schema_ref(), &task.project_field_ids);

    if let Some(batch_size) = batch_size {
        record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
    }

    // Apply positional deletes as row selections.
    let row_selection = if let Some(positional_delete_indexes) = task.positional_deletes {
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

    // Build the batch stream and send all the RecordBatches that it generates
    // to the requester.
    let file_path = task.data_file_path.clone();
    println!("Reading data file: {}", file_path);
    let record_batch_stream = record_batch_stream_builder
        .build()?
        .map(move |batch| match batch {
            Ok(batch) => record_batch_transformer.process_record_batch(batch),
            Err(err) => Err(err.into()),
        });

    Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
}

fn process_incremental_delete_task(
    file_path: String,
    delete_vector: RoaringTreemap,
    batch_size: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let stream = futures::stream::iter(delete_vector.into_iter())
        .chunks(batch_size.unwrap_or(1024))
        .map(move |chunk| {
            let array = UInt64Array::from_iter(chunk.into_iter());
            RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![Field::new(
                    "pos",
                    DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(array)],
            )
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create RecordBatch for DeleteVector",
                )
            })
            .and_then(|batch| ArrowReader::add_file_path_column(batch, &file_path))
        });

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}
