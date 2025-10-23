//! Incremental table scan implementation.

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::delete_filter::DeleteFilter;
use crate::arrow::{ArrowBatchEmitter, ArrowReaderBuilder, IncrementalArrowBatchRecordStream};
use crate::delete_file_index::DeleteFileIndex;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, DeleteFileContext};
use crate::scan::cache::{ExpressionEvaluatorCache, ManifestEvaluatorCache, PartitionFilterCache};
use crate::scan::context::ManifestEntryContext;
use crate::spec::{DataContentType, ManifestStatus, Snapshot, SnapshotRef};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind, Result};

mod context;
use context::*;
mod task;
use futures::channel::mpsc::{Sender, channel};
use futures::{SinkExt, StreamExt, TryStreamExt};
use itertools::Itertools;
pub use task::*;

use crate::runtime::spawn;

/// Builder for an incremental table scan.
#[derive(Debug)]
pub struct IncrementalTableScanBuilder<'a> {
    table: &'a Table,
    // Defaults to `None`, which means all columns.
    column_names: Option<Vec<String>>,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: i64,
    batch_size: Option<usize>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl<'a> IncrementalTableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table, from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            table,
            column_names: None,
            from_snapshot_id: Some(from_snapshot_id),
            to_snapshot_id,
            batch_size: None,
            concurrency_limit_data_files: num_cpus,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
        }
    }

    /// Set the batch size for reading data files.
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Select all columns of the table.
    pub fn select_all(mut self) -> Self {
        self.column_names = None;
        self
    }

    /// Select no columns of the table.
    pub fn select_empty(mut self) -> Self {
        self.column_names = Some(vec![]);
        self
    }

    /// Select some columns of the table.
    pub fn select(mut self, column_names: impl IntoIterator<Item = impl ToString>) -> Self {
        self.column_names = Some(
            column_names
                .into_iter()
                .map(|item| item.to_string())
                .collect(),
        );
        self
    }

    /// Set the `from_snapshot_id` for the incremental scan.
    pub fn from_snapshot_id(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id = Some(from_snapshot_id);
        self
    }

    /// Set the `to_snapshot_id` for the incremental scan.
    pub fn to_snapshot_id(mut self, to_snapshot_id: i64) -> Self {
        self.to_snapshot_id = to_snapshot_id;
        self
    }

    /// Set the concurrency limit for reading data files.
    pub fn with_concurrency_limit_data_files(mut self, limit: usize) -> Self {
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Set the concurrency limit for reading manifest entries.
    pub fn with_concurrency_limit_manifest_entries(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_entries = limit;
        self
    }

    /// Set the concurrency limit for reading manifest files.
    pub fn with_concurrency_limit_manifest_files(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_files = limit;
        self
    }

    /// Build the incremental table scan.
    pub fn build(self) -> Result<IncrementalTableScan> {
        let snapshot_from: Option<Arc<Snapshot>> = match self.from_snapshot_id {
            Some(id) => Some(
                self.table
                    .metadata()
                    .snapshot_by_id(id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Snapshot with id {} not found", id),
                        )
                    })?
                    .clone(),
            ),
            None => None,
        };

        let snapshot_to: Arc<Snapshot> = self
            .table
            .metadata()
            .snapshot_by_id(self.to_snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot with id {} not found", self.to_snapshot_id),
                )
            })?
            .clone();

        // TODO: What properties do we need to verify about the snapshots? What about
        // schema changes?
        let snapshots = ancestors_between(
            &self.table.metadata_ref(),
            snapshot_to.snapshot_id(),
            snapshot_from.as_ref().map(|s| s.snapshot_id()),
        )
        .collect_vec();

        assert_eq!(
            snapshots.first().map(|s| s.snapshot_id()),
            Some(snapshot_to.snapshot_id())
        );

        let schema = snapshot_to.schema(self.table.metadata())?;

        if let Some(column_names) = self.column_names.as_ref() {
            for column_name in column_names {
                if schema.field_by_name(column_name).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Column {} not found in table. Schema: {}",
                            column_name, schema
                        ),
                    ));
                }
            }
        }

        let mut field_ids = vec![];
        let column_names = self.column_names.clone().unwrap_or_else(|| {
            schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect()
        });

        for column_name in column_names.iter() {
            let field_id = schema.field_id_by_name(column_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Column {} not found in table. Schema: {}",
                        column_name, schema
                    ),
                )
            })?;

            schema
                .as_struct()
                .field_by_id(field_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Column {} is not a direct child of schema but a nested field, which is not supported now. Schema: {}",
                            column_name, schema
                        ),
                    )
                })?;

            field_ids.push(field_id);
        }

        let plan_context = IncrementalPlanContext {
            snapshots,
            from_snapshot: snapshot_from,
            table_metadata: self.table.metadata_ref(),
            to_snapshot_schema: schema,
            object_cache: self.table.object_cache().clone(),
            field_ids: Arc::new(field_ids),
            partition_filter_cache: Arc::new(PartitionFilterCache::new()),
            manifest_evaluator_cache: Arc::new(ManifestEvaluatorCache::new()),
            expression_evaluator_cache: Arc::new(ExpressionEvaluatorCache::new()),
            caching_delete_file_loader: CachingDeleteFileLoader::new(
                self.table.file_io().clone(),
                self.concurrency_limit_data_files,
            ),
        };

        Ok(IncrementalTableScan {
            plan_context,
            file_io: self.table.file_io().clone(),
            column_names: Some(column_names),
            batch_size: self.batch_size,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
        })
    }
}

/// An incremental table scan.
#[derive(Debug)]
pub struct IncrementalTableScan {
    plan_context: IncrementalPlanContext,
    file_io: FileIO,
    column_names: Option<Vec<String>>,
    batch_size: Option<usize>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl IncrementalTableScan {
    /// Returns the optional `from` snapshot of this incremental table scan.
    pub fn snapshot_from(&self) -> Option<&SnapshotRef> {
        self.plan_context.from_snapshot.as_ref()
    }

    /// Returns the snapshots involved in this incremental table scan.
    pub fn snapshots(&self) -> &[SnapshotRef] {
        &self.plan_context.snapshots
    }

    /// Returns the `to` snapshot of this incremental table scan.
    pub fn snapshot_to(&self) -> &SnapshotRef {
        self.snapshots()
            .first()
            .expect("There is always at least one snapshot")
    }

    /// Returns the selected column names of this incremental table scan.
    /// If `None`, all columns are selected.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Plans the files to be read in this incremental table scan.
    pub async fn plan_files(&self) -> Result<IncrementalFileScanTaskStream> {
        let concurrency_limit_manifest_files = self.concurrency_limit_manifest_files;
        let concurrency_limit_manifest_entries = self.concurrency_limit_manifest_entries;

        // Used to stream `ManifestEntryContexts` between stages of the planning operation.
        let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) =
            channel(concurrency_limit_manifest_files);
        let (manifest_entry_delete_ctx_tx, manifest_entry_delete_ctx_rx) =
            channel(concurrency_limit_manifest_files);

        // Used to stream the results back to the caller.
        let (file_scan_task_tx, file_scan_task_rx) = channel(concurrency_limit_manifest_entries);

        let (delete_file_idx, delete_file_tx) = DeleteFileIndex::new();

        let manifest_file_contexts = self
            .plan_context
            .build_manifest_file_contexts(
                manifest_entry_data_ctx_tx,
                delete_file_idx.clone(),
                manifest_entry_delete_ctx_tx,
            )
            .await?;

        let mut channel_for_manifest_error: Sender<Result<_>> = file_scan_task_tx.clone();

        // Concurrently load all [`Manifest`]s and stream their [`ManifestEntry`]s
        spawn(async move {
            let result = futures::stream::iter(manifest_file_contexts)
                .try_for_each_concurrent(concurrency_limit_manifest_files, |ctx| async move {
                    ctx.fetch_manifest_and_stream_manifest_entries().await
                })
                .await;

            if let Err(error) = result {
                let _ = channel_for_manifest_error.send(Err(error)).await;
            }
        });

        let mut channel_for_data_manifest_entry_error = file_scan_task_tx.clone();
        let mut channel_for_delete_manifest_entry_error = file_scan_task_tx.clone();

        // Process the delete file [`ManifestEntry`] stream in parallel
        spawn(async move {
            let result = manifest_entry_delete_ctx_rx
                .map(|me_ctx| Ok((me_ctx, delete_file_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| async move {
                        spawn(async move {
                            Self::process_delete_manifest_entry(tx, manifest_entry_context).await
                        })
                        .await
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_delete_manifest_entry_error
                    .send(Err(error))
                    .await;
            }
        })
        .await;

        // TODO: Streaming this into the delete index seems somewhat redundant, as we
        // could directly stream into the CachingDeleteFileLoader and instantly load the
        // delete files.
        let positional_deletes = delete_file_idx.positional_deletes().await;
        let result = self
            .plan_context
            .caching_delete_file_loader
            .load_deletes(
                &positional_deletes,
                self.plan_context.to_snapshot_schema.clone(),
            )
            .await;

        let delete_filter = match result {
            Ok(loaded_deletes) => loaded_deletes.unwrap(),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load positional deletes: {}", e),
                ));
            }
        };

        // Process the data file [`ManifestEntry`] stream in parallel
        let filter = delete_filter.clone();
        spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(|me_ctx| Ok((me_ctx, file_scan_task_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| {
                        let filter = filter.clone();
                        async move {
                            if manifest_entry_context.manifest_entry.status()
                                == ManifestStatus::Added
                            {
                                spawn(async move {
                                    Self::process_data_manifest_entry(
                                        tx,
                                        manifest_entry_context,
                                        &filter,
                                    )
                                    .await
                                })
                                .await
                            } else if manifest_entry_context.manifest_entry.status()
                                == ManifestStatus::Deleted
                            {
                                // TODO: Process deleted files
                                Ok(())
                            } else {
                                Ok(())
                            }
                        }
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_data_manifest_entry_error.send(Err(error)).await;
            }
        });

        let mut tasks = file_scan_task_rx.try_collect::<Vec<_>>().await?;

        let appended_files = tasks
            .iter()
            .filter_map(|task| match task {
                IncrementalFileScanTask::Append(append_task) => {
                    Some(append_task.data_file_path.clone())
                }
                _ => None,
            })
            .collect::<HashSet<String>>();

        delete_filter.with_read(|state| {
            for (path, delete_vector) in state.delete_vectors().iter() {
                if !appended_files.contains::<String>(path) {
                    let delete_task =
                        IncrementalFileScanTask::Delete(path.clone(), delete_vector.clone());
                    tasks.push(delete_task);
                }
            }
            Ok(())
        })?;

        for task in tasks.iter() {
            match task {
                IncrementalFileScanTask::Append(append_task) => {
                    println!(
                        "Planned incremental append file scan task: {:?}, deletes: {:?}",
                        append_task.data_file_path, append_task.positional_deletes,
                    );
                }
                IncrementalFileScanTask::Delete(delete_path, _) => {
                    println!(
                        "Planned incremental delete file scan task: {:?}",
                        delete_path,
                    );
                }
            }
        }

        // We actually would not need a stream here, but we can keep it compatible with
        // other scan types.
        Ok(futures::stream::iter(tasks).map(|t| Ok(t)).boxed())
    }

    /// Returns an [`IncrementalArrowBatchRecordStream`] for this incremental table scan.
    pub async fn to_arrow(&self) -> Result<IncrementalArrowBatchRecordStream> {
        let file_scan_task_stream = self.plan_files().await?;
        let mut arrow_reader_builder  =
            ArrowReaderBuilder::new(self.file_io.clone())
                .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
                .with_row_group_filtering_enabled(true)
                .with_row_selection_enabled(true);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        let arrow_reader = arrow_reader_builder.build();
        file_scan_task_stream.read(arrow_reader)
    }

    async fn process_delete_manifest_entry(
        mut delete_file_ctx_tx: Sender<DeleteFileContext>,
        manifest_entry_context: ManifestEntryContext,
    ) -> Result<()> {
        // Skip processing this manifest entry if it has been marked as deleted.
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(());
        }

        // Abort the plan if we encounter a manifest entry for a data file
        if manifest_entry_context.manifest_entry.content_type() == DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a data file in a delete file manifest",
            ));
        }

        delete_file_ctx_tx
            .send(DeleteFileContext {
                manifest_entry: manifest_entry_context.manifest_entry.clone(),
                partition_spec_id: manifest_entry_context.partition_spec_id,
            })
            .await?;
        Ok(())
    }

    async fn process_data_manifest_entry(
        mut file_scan_task_tx: Sender<Result<IncrementalFileScanTask>>,
        manifest_entry_context: ManifestEntryContext,
        delete_filter: &DeleteFilter,
    ) -> Result<()> {
        // Skip processing this manifest entry if it has been marked as deleted.
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(());
        }

        // Abort the plan if we encounter a manifest entry for a delete file
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in a data file manifest",
            ));
        }

        let file_scan_task = IncrementalFileScanTask::append_from_manifest_entry(
            &manifest_entry_context,
            delete_filter,
        );

        file_scan_task_tx.send(Ok(file_scan_task)).await?;
        Ok(())
    }
}
