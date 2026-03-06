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

use std::sync::{Arc, Mutex};

use futures::stream::BoxStream;

use crate::Result;
use crate::arrow::delete_filter::{DeleteFilter, is_equality_delete};
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate;
use crate::scan::context::ManifestEntryContext;
use crate::spec::{DataFileFormat, PartitionSpec, Schema, SchemaRef, Struct, TableMetadataRef};

/// Base file scan task containing common attributes for incremental scan tasks.
#[derive(Debug, Clone)]
pub struct BaseIncrementalFileScanTask {
    /// The total size of the data file in bytes.
    pub file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file.
    pub record_count: Option<u64>,
    /// The path to the data file to scan.
    pub data_file_path: String,
    /// The format of the data file to scan.
    pub data_file_format: DataFileFormat,
    /// The schema of the data file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// Partition data from the manifest entry, used to supply constant values for
    /// identity-transformed partition columns without reading them from the file.
    pub partition: Option<Struct>,
    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms.
    pub partition_spec: Option<Arc<PartitionSpec>>,
}

impl BaseIncrementalFileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// A file scan task for appended data files in an incremental scan.
#[derive(Debug, Clone)]
pub struct AppendedFileScanTask {
    /// The base file scan task attributes.
    pub base: BaseIncrementalFileScanTask,
    /// The optional positional deletes associated with this data file.
    pub positional_deletes: Option<Arc<Mutex<DeleteVector>>>,
    /// The combined equality delete predicate for rows to filter out from this appended file.
    /// Pre-computed during planning; `None` if no equality deletes apply.
    pub equality_delete_predicate: Option<Predicate>,
}

impl AppendedFileScanTask {
    /// Returns the data file path of this appended file scan task.
    pub fn data_file_path(&self) -> &str {
        self.base.data_file_path()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        self.base.schema()
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.base.schema_ref()
    }
}

/// A file scan task for deleted data files in an incremental scan.
#[derive(Debug, Clone)]
pub struct DeletedFileScanTask {
    /// The base file scan task attributes.
    pub base: BaseIncrementalFileScanTask,
}

impl DeletedFileScanTask {
    /// Returns the data file path of this deleted file scan task.
    pub fn data_file_path(&self) -> &str {
        self.base.data_file_path()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        self.base.schema()
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.base.schema_ref()
    }
}

/// A file scan task for equality deletes applied to an existing data file in an incremental scan.
#[derive(Debug, Clone)]
pub struct EqualityDeleteScanTask {
    /// The base file scan task attributes.
    pub base: BaseIncrementalFileScanTask,
    /// The combined equality delete predicate for all equality delete files.
    /// Pre-computed during task creation to avoid recomputing in the arrow reader.
    pub combined_predicate: Predicate,
}

impl EqualityDeleteScanTask {
    /// Returns the data file path of this equality delete scan task.
    pub fn data_file_path(&self) -> &str {
        self.base.data_file_path()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        self.base.schema()
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.base.schema_ref()
    }
}

/// The streams of appended and deleted file scan tasks.
pub type IncrementalFileScanTaskStreams = (
    BoxStream<'static, Result<AppendedFileScanTask>>,
    BoxStream<'static, Result<DeleteScanTask>>,
);

/// A delete scan task, which can be a deleted data file, positional deletes, or equality deletes.
#[derive(Debug, Clone)]
pub enum DeleteScanTask {
    /// A deleted data file.
    DeletedFile(DeletedFileScanTask),
    /// Positional deletes (deleted records of a data file). First argument is the file path,
    /// second the delete vector.
    PositionalDeletes(String, DeleteVector),
    /// Equality deletes applied to an existing data file.
    EqualityDeletes(EqualityDeleteScanTask),
}

/// An incremental file scan task, which can be an appended data file, deleted data file,
/// or positional deletes.
#[derive(Debug, Clone)]
pub enum IncrementalFileScanTask {
    /// An appended data file.
    Append(AppendedFileScanTask),
    /// A deleted data file.
    Delete(DeletedFileScanTask),
    /// Positional deletes (deleted records of a data file). First argument is the file path,
    /// second the delete vector.
    PositionalDeletes(String, DeleteVector),
}

impl IncrementalFileScanTask {
    /// Create an `IncrementalFileScanTask::Append` from a `ManifestEntryContext` and `DeleteFilter`.
    /// Queries the DeleteFileIndex to populate equality deletes that apply to this data file.
    pub(crate) async fn append_from_manifest_entry(
        manifest_entry_context: &ManifestEntryContext,
        delete_filter: &DeleteFilter,
        table_metadata: &TableMetadataRef,
    ) -> Result<Self> {
        let data_file_path = manifest_entry_context.manifest_entry.file_path();

        // Query the delete file index to get all deletes that apply to this data file.
        // Pass the file's sequence number to ensure only newer deletes are included.
        let all_deletes = manifest_entry_context
            .delete_file_index
            .get_deletes_for_data_file(
                manifest_entry_context.manifest_entry.data_file(),
                manifest_entry_context.manifest_entry.sequence_number(),
            )
            .await;

        // Filter to get only equality deletes and eagerly build the combined predicate.
        let equality_deletes: Vec<_> = all_deletes.into_iter().filter(is_equality_delete).collect();
        let equality_delete_predicate = if equality_deletes.is_empty() {
            None
        } else {
            delete_filter
                .build_combined_equality_delete_predicate(&equality_deletes)
                .await?
        };

        let partition_spec_id = manifest_entry_context.partition_spec_id;
        let partition_spec = table_metadata
            .partition_spec_by_id(partition_spec_id)
            .cloned();
        let partition = Some(
            manifest_entry_context
                .manifest_entry
                .data_file()
                .partition()
                .clone(),
        );

        Ok(IncrementalFileScanTask::Append(AppendedFileScanTask {
            base: BaseIncrementalFileScanTask {
                file_size_in_bytes: manifest_entry_context.manifest_entry.file_size_in_bytes(),
                start: 0,
                length: manifest_entry_context.manifest_entry.file_size_in_bytes(),
                record_count: Some(manifest_entry_context.manifest_entry.record_count()),
                data_file_path: data_file_path.to_string(),
                data_file_format: manifest_entry_context.manifest_entry.file_format(),
                schema: manifest_entry_context.snapshot_schema.clone(),
                project_field_ids: manifest_entry_context.field_ids.as_ref().clone(),
                partition,
                partition_spec,
            },
            positional_deletes: delete_filter.get_delete_vector_for_path(data_file_path),
            equality_delete_predicate,
        }))
    }

    /// Returns the data file path of this incremental file scan task.
    pub fn data_file_path(&self) -> &str {
        match self {
            IncrementalFileScanTask::Append(task) => task.data_file_path(),
            IncrementalFileScanTask::Delete(task) => task.data_file_path(),
            IncrementalFileScanTask::PositionalDeletes(path, _) => path,
        }
    }
}
