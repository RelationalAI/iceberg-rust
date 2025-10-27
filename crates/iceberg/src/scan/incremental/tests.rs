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

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use uuid::Uuid;

use crate::TableIdent;
use crate::io::{FileIO, OutputFile};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestEntry, ManifestListWriter,
    ManifestStatus, ManifestWriterBuilder, PartitionSpec, Struct, TableMetadata,
};
use crate::table::Table;

/// Represents an operation to perform on a snapshot.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Add rows with the given `n` values and `data` values. Example: `Add(vec![1, 2, 3],
    /// vec!["a", "b", "c"])` adds three rows with n=1,2,3 and data="a","b","c"
    Add(Vec<i32>, Vec<String>),

    /// Delete rows by their n values (uses positional deletes).
    /// Example: `Delete(vec![2])` deletes the row where n=2
    Delete(Vec<i32>),
}

/// Tracks the state of data files across snapshots
#[derive(Debug, Clone)]
struct DataFileInfo {
    path: String,
    snapshot_id: i64,
    sequence_number: i64,
    n_values: Vec<i32>,
}

/// Test fixture that creates a table with custom snapshots based on operations.
///
/// # Example
/// ```
/// let fixture = IncrementalTestFixture::new(vec![
///     Operation::Add(vec![], vec![]),                     // Empty snapshot
///     Operation::Add(vec![1, 2, 3], vec!["1", "2", "3"]), // Add 3 rows
///     Operation::Delete(vec![2]),                         // Delete row with n=2
/// ])
/// .await;
/// ```
pub struct IncrementalTestFixture {
    pub table_location: String,
    pub table: Table,
    _tmp_dir: TempDir, // Keep temp dir alive
}

impl IncrementalTestFixture {
    /// Create a new test fixture with the given operations.
    pub async fn new(operations: Vec<Operation>) -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("incremental_test_table");

        // Create directory structure
        fs::create_dir_all(table_location.join("metadata")).unwrap();
        fs::create_dir_all(table_location.join("data")).unwrap();

        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let num_snapshots = operations.len();
        let current_snapshot_id = num_snapshots as i64;
        let last_sequence_number = (num_snapshots - 1) as i64;

        // Build the snapshots JSON dynamically
        let mut snapshots_json = Vec::new();
        let mut snapshot_log_json = Vec::new();
        let mut manifest_list_locations = Vec::new();

        for (i, op) in operations.iter().enumerate() {
            let snapshot_id = (i + 1) as i64;
            let parent_id = if i == 0 { None } else { Some(i as i64) };
            let sequence_number = i as i64;
            let timestamp = 1515100955770 + (i as i64 * 1000);

            let operation_type = match op {
                Operation::Add(..) => "append",
                Operation::Delete(..) => "delete",
            };

            let manifest_list_location =
                table_location.join(format!("metadata/snap-{}-manifest-list.avro", snapshot_id));
            manifest_list_locations.push(manifest_list_location.clone());

            let parent_str = if let Some(pid) = parent_id {
                format!(r#""parent-snapshot-id": {},"#, pid)
            } else {
                String::new()
            };

            snapshots_json.push(format!(
                r#"    {{
      "snapshot-id": {},
      {}
      "timestamp-ms": {},
      "sequence-number": {},
      "summary": {{"operation": "{}"}},
      "manifest-list": "{}",
      "schema-id": 0
    }}"#,
                snapshot_id,
                parent_str,
                timestamp,
                sequence_number,
                operation_type,
                manifest_list_location.display()
            ));

            snapshot_log_json.push(format!(
                r#"    {{"snapshot-id": {}, "timestamp-ms": {}}}"#,
                snapshot_id, timestamp
            ));
        }

        let snapshots_str = snapshots_json.join(",\n");
        let snapshot_log_str = snapshot_log_json.join(",\n");

        // Create the table metadata
        let metadata_json = format!(
            r#"{{
  "format-version": 2,
  "table-uuid": "{}",
  "location": "{}",
  "last-sequence-number": {},
  "last-updated-ms": 1602638573590,
  "last-column-id": 2,
  "current-schema-id": 0,
  "schemas": [
    {{
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {{"id": 1, "name": "n", "required": true, "type": "int"}},
        {{"id": 2, "name": "data", "required": true, "type": "string"}}
      ]
    }}
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {{
      "spec-id": 0,
      "fields": []
    }}
  ],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [
    {{
      "order-id": 0,
      "fields": []
    }}
  ],
  "properties": {{}},
  "current-snapshot-id": {},
  "snapshots": [
{}
  ],
  "snapshot-log": [
{}
  ],
  "metadata-log": []
}}"#,
            Uuid::new_v4(),
            table_location.display(),
            last_sequence_number,
            current_snapshot_id,
            snapshots_str,
            snapshot_log_str
        );

        let table_metadata_location = table_location.join("metadata/v1.json");
        let table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "incremental_test"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata_location.as_os_str().to_str().unwrap())
            .build()
            .unwrap();

        let mut fixture = Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
            _tmp_dir: tmp_dir,
        };

        // Setup all snapshots based on operations
        fixture.setup_snapshots(operations).await;

        fixture
    }

    fn next_manifest_file(&self) -> OutputFile {
        self.table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_{}.avro",
                self.table_location,
                Uuid::new_v4()
            ))
            .unwrap()
    }

    async fn setup_snapshots(&mut self, operations: Vec<Operation>) {
        let current_schema = self
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .schema(self.table.metadata())
            .unwrap();
        let partition_spec = Arc::new(PartitionSpec::unpartition_spec());
        let empty_partition = Struct::empty();

        // Track all data files and their contents across snapshots
        let mut data_files: Vec<DataFileInfo> = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut delete_files: Vec<(String, i64, i64, Vec<(String, i64)>)> = Vec::new(); // (path, snapshot_id, sequence_number, [(data_file_path, position)])

        for (snapshot_idx, operation) in operations.iter().enumerate() {
            let snapshot_id = (snapshot_idx + 1) as i64;
            let sequence_number = snapshot_idx as i64;
            let parent_snapshot_id = if snapshot_idx == 0 {
                None
            } else {
                Some(snapshot_idx as i64)
            };

            match operation {
                Operation::Add(n_values, data_values) => {
                    // Create data manifest
                    let mut data_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_data();

                    // Add existing data files from previous snapshots
                    for data_file in &data_files {
                        data_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(data_file.snapshot_id)
                                    .sequence_number(data_file.sequence_number)
                                    .file_sequence_number(data_file.sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::Data)
                                            .file_path(data_file.path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(1024)
                                            .record_count(data_file.n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    // Add new data if not empty
                    if !n_values.is_empty() {
                        let data_file_path =
                            format!("{}/data/data-{}.parquet", &self.table_location, snapshot_id);
                        self.write_parquet_file(&data_file_path, n_values, data_values)
                            .await;

                        data_writer
                            .add_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Added)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::Data)
                                            .file_path(data_file_path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(1024)
                                            .record_count(n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();

                        // Track this data file
                        data_files.push(DataFileInfo {
                            path: data_file_path,
                            snapshot_id,
                            sequence_number,
                            n_values: n_values.clone(),
                        });
                    }

                    let data_manifest = data_writer.write_manifest_file().await.unwrap();

                    // Create delete manifest if there are any delete files
                    let mut manifests = vec![data_manifest];
                    if !delete_files.is_empty() {
                        let mut delete_writer = ManifestWriterBuilder::new(
                            self.next_manifest_file(),
                            Some(snapshot_id),
                            None,
                            current_schema.clone(),
                            partition_spec.as_ref().clone(),
                        )
                        .build_v2_deletes();

                        for (delete_path, del_snapshot_id, del_sequence_number, _) in &delete_files
                        {
                            let delete_count = delete_files
                                .iter()
                                .filter(|(p, _, _, _)| p == delete_path)
                                .map(|(_, _, _, deletes)| deletes.len())
                                .sum::<usize>();

                            delete_writer
                                .add_existing_entry(
                                    ManifestEntry::builder()
                                        .status(ManifestStatus::Existing)
                                        .snapshot_id(*del_snapshot_id)
                                        .sequence_number(*del_sequence_number)
                                        .file_sequence_number(*del_sequence_number)
                                        .data_file(
                                            DataFileBuilder::default()
                                                .partition_spec_id(0)
                                                .content(DataContentType::PositionDeletes)
                                                .file_path(delete_path.clone())
                                                .file_format(DataFileFormat::Parquet)
                                                .file_size_in_bytes(512)
                                                .record_count(delete_count as u64)
                                                .partition(empty_partition.clone())
                                                .key_metadata(None)
                                                .build()
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .unwrap();
                        }

                        manifests.push(delete_writer.write_manifest_file().await.unwrap());
                    }

                    // Write manifest list
                    let mut manifest_list_write = ManifestListWriter::v2(
                        self.table
                            .file_io()
                            .new_output(format!(
                                "{}/metadata/snap-{}-manifest-list.avro",
                                self.table_location, snapshot_id
                            ))
                            .unwrap(),
                        snapshot_id,
                        parent_snapshot_id,
                        sequence_number,
                    );
                    manifest_list_write
                        .add_manifests(manifests.into_iter())
                        .unwrap();
                    manifest_list_write.close().await.unwrap();
                }

                Operation::Delete(n_values_to_delete) => {
                    // Find positions to delete from each data file
                    let mut deletes_by_file: HashMap<String, Vec<i64>> = HashMap::new();

                    for n_to_delete in n_values_to_delete {
                        for data_file in &data_files {
                            if let Some(pos) =
                                data_file.n_values.iter().position(|n| n == n_to_delete)
                            {
                                deletes_by_file
                                    .entry(data_file.path.clone())
                                    .or_default()
                                    .push(pos as i64);
                            }
                        }
                    }

                    // Create data manifest with existing data files
                    let mut data_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_data();

                    for data_file in &data_files {
                        data_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(data_file.snapshot_id)
                                    .sequence_number(data_file.sequence_number)
                                    .file_sequence_number(data_file.sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::Data)
                                            .file_path(data_file.path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(1024)
                                            .record_count(data_file.n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    let data_manifest = data_writer.write_manifest_file().await.unwrap();

                    // Create delete manifest
                    let mut delete_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_deletes();

                    // Add existing delete files
                    for (delete_path, del_snapshot_id, del_sequence_number, _) in &delete_files {
                        let delete_count = delete_files
                            .iter()
                            .filter(|(p, _, _, _)| p == delete_path)
                            .map(|(_, _, _, deletes)| deletes.len())
                            .sum::<usize>();

                        delete_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(*del_snapshot_id)
                                    .sequence_number(*del_sequence_number)
                                    .file_sequence_number(*del_sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::PositionDeletes)
                                            .file_path(delete_path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(512)
                                            .record_count(delete_count as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    // Add new delete files
                    for (data_file_path, positions) in deletes_by_file {
                        let delete_file_path = format!(
                            "{}/data/delete-{}-{}.parquet",
                            &self.table_location,
                            snapshot_id,
                            Uuid::new_v4()
                        );
                        self.write_positional_delete_file(
                            &delete_file_path,
                            &data_file_path,
                            &positions,
                        )
                        .await;

                        delete_writer
                            .add_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Added)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::PositionDeletes)
                                            .file_path(delete_file_path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(512)
                                            .record_count(positions.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();

                        // Track this delete file
                        delete_files.push((
                            delete_file_path,
                            snapshot_id,
                            sequence_number,
                            positions
                                .into_iter()
                                .map(|pos| (data_file_path.clone(), pos))
                                .collect(),
                        ));
                    }

                    let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

                    // Write manifest list
                    let mut manifest_list_write = ManifestListWriter::v2(
                        self.table
                            .file_io()
                            .new_output(format!(
                                "{}/metadata/snap-{}-manifest-list.avro",
                                self.table_location, snapshot_id
                            ))
                            .unwrap(),
                        snapshot_id,
                        parent_snapshot_id,
                        sequence_number,
                    );
                    manifest_list_write
                        .add_manifests(vec![data_manifest, delete_manifest].into_iter())
                        .unwrap();
                    manifest_list_write.close().await.unwrap();
                }
            }
        }
    }

    async fn write_parquet_file(&self, path: &str, n_values: &[i32], data_values: &[String]) {
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("n", arrow_schema::DataType::Int32, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                ),
                arrow_schema::Field::new("data", arrow_schema::DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let col_n = Arc::new(Int32Array::from(n_values.to_vec())) as ArrayRef;
        let col_data = Arc::new(StringArray::from(data_values.to_vec())) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![col_n, col_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn write_positional_delete_file(
        &self,
        path: &str,
        data_file_path: &str,
        positions: &[i64],
    ) {
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2147483546".to_string(),
                    )])),
                arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2147483545".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let file_paths: Vec<&str> = vec![data_file_path; positions.len()];
        let col_file_path = Arc::new(StringArray::from(file_paths)) as ArrayRef;
        let col_pos = Arc::new(arrow_array::Int64Array::from(positions.to_vec())) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![col_file_path, col_pos]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Verify incremental scan results.
    ///
    /// Verifies that the incremental scan contains the expected appended and deleted records.
    pub async fn verify_incremental_scan(
        &self,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
        expected_appends: Vec<(i32, &str)>,
        expected_deletes: Vec<(i32, &str)>,
    ) {
        use arrow_array::cast::AsArray;
        use arrow_select::concat::concat_batches;
        use futures::TryStreamExt;

        let incremental_scan = self
            .table
            .incremental_scan(from_snapshot_id, to_snapshot_id)
            .build()
            .unwrap();

        let stream = incremental_scan.to_arrow().await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();

        // Separate appends and deletes
        let append_batches: Vec<_> = batches
            .iter()
            .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
            .map(|(_, b)| b.clone())
            .collect();

        let delete_batches: Vec<_> = batches
            .iter()
            .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Delete)
            .map(|(_, b)| b.clone())
            .collect();

        // Verify appended records
        if !append_batches.is_empty() {
            let append_batch =
                concat_batches(&append_batches[0].schema(), append_batches.iter()).unwrap();

            let n_array = append_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let data_array = append_batch.column(1).as_string::<i32>();

            let mut appended_pairs: Vec<(i32, String)> = (0..append_batch.num_rows())
                .map(|i| (n_array.value(i), data_array.value(i).to_string()))
                .collect();
            appended_pairs.sort();

            let expected_appends: Vec<(i32, String)> = expected_appends
                .into_iter()
                .map(|(n, s)| (n, s.to_string()))
                .collect();

            assert_eq!(appended_pairs, expected_appends);
        } else {
            assert!(expected_appends.is_empty(), "Expected appends but got none");
        }

        // Verify deleted records
        if !delete_batches.is_empty() {
            let delete_batch =
                concat_batches(&delete_batches[0].schema(), delete_batches.iter()).unwrap();

            let n_array = delete_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let data_array = delete_batch.column(1).as_string::<i32>();

            let mut deleted_pairs: Vec<(i32, String)> = (0..delete_batch.num_rows())
                .map(|i| (n_array.value(i), data_array.value(i).to_string()))
                .collect();
            deleted_pairs.sort();

            let expected_deletes: Vec<(i32, String)> = expected_deletes
                .into_iter()
                .map(|(n, s)| (n, s.to_string()))
                .collect();

            assert_eq!(deleted_pairs, expected_deletes);
        } else {
            assert!(expected_deletes.is_empty(), "Expected deletes but got none");
        }
    }
}

#[tokio::test]
async fn test_incremental_fixture_simple() {
    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], vec![]),
        Operation::Add(vec![1, 2, 3], vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
        ]),
        Operation::Delete(vec![2]),
    ])
    .await;

    // Verify we have 3 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 3);

    // Verify snapshot IDs
    assert_eq!(snapshots[0].snapshot_id(), 1);
    assert_eq!(snapshots[1].snapshot_id(), 2);
    assert_eq!(snapshots[2].snapshot_id(), 3);

    // Verify parent relationships
    assert_eq!(snapshots[0].parent_snapshot_id(), None);
    assert_eq!(snapshots[1].parent_snapshot_id(), Some(1));
    assert_eq!(snapshots[2].parent_snapshot_id(), Some(2));

    // Verify incremental scan from snapshot 1 to snapshot 3
    // Expected appends: snapshot 2 adds [1, 2, 3]
    // Expected deletes: snapshot 3 deletes [2]
    // In total we expect appends [1, 3] and deletes []
    fixture
        .verify_incremental_scan(1, 3, vec![(1, "1"), (3, "3")], vec![])
        .await;
}

#[tokio::test]
async fn test_incremental_fixture_complex() {
    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], vec![]), // Snapshot 1: Empty
        Operation::Add(vec![1, 2, 3, 4, 5], vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ]), // Snapshot 2: Add 5 rows
        Operation::Delete(vec![2, 4]),  // Snapshot 3: Delete rows with n=2,4
        Operation::Add(vec![6, 7], vec!["f".to_string(), "g".to_string()]), // Snapshot 4: Add 2 more rows
        Operation::Delete(vec![1, 3, 5, 6, 7]), // Snapshot 5: Delete all remaining rows
    ])
    .await;

    // Verify we have 5 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 5);

    // Verify parent chain
    assert_eq!(snapshots[0].parent_snapshot_id(), None);
    for (i, snapshot) in snapshots.iter().enumerate().take(5).skip(1) {
        assert_eq!(snapshot.parent_snapshot_id(), Some(i as i64));
    }

    // Verify current snapshot
    assert_eq!(
        fixture
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id(),
        5
    );

    // Verify incremental scan from snapshot 1 to snapshot 5
    // All data has been deleted, so we expect the empty result.
    fixture.verify_incremental_scan(1, 5, vec![], vec![]).await;
}
