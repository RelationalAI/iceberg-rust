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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// OverwriteAction is a transaction action for overwriting data files in the table.
///
/// Creates a snapshot with `Operation::Overwrite` semantics — adds new data files and
/// optionally removes existing data files by rewriting affected manifests with those
/// entries marked as `ManifestStatus::Deleted`.
pub struct OverwriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if !self.deleted_data_files.is_empty() && table.metadata().current_snapshot().is_none() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot delete data files from a table with no current snapshot",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let deleted_file_paths: HashSet<String> = self
            .deleted_data_files
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        let snapshot_id = snapshot_producer.snapshot_id();
        snapshot_producer
            .commit(
                OverwriteOperation {
                    deleted_file_paths,
                    snapshot_id,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct OverwriteOperation {
    deleted_file_paths: HashSet<String>,
    snapshot_id: i64,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        if self.deleted_file_paths.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| {
                    entry.has_added_files()
                        || entry.has_existing_files()
                        || entry.has_deleted_files()
                })
                .cloned()
                .collect());
        }

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files()
                && !manifest_file.has_existing_files()
                && !manifest_file.has_deleted_files()
            {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let has_deletes = manifest.entries().iter().any(|entry| {
                entry.is_alive() && self.deleted_file_paths.contains(entry.file_path())
            });

            if has_deletes {
                let rewritten = self
                    .rewrite_manifest(snapshot_produce, manifest_file, &manifest)
                    .await?;
                result.push(rewritten);
            } else {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }
}

impl OverwriteOperation {
    /// Rewrite a manifest, marking entries whose file paths are in `deleted_file_paths`
    /// as `ManifestStatus::Deleted`.
    async fn rewrite_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifest_file: &ManifestFile,
        manifest: &crate::spec::Manifest,
    ) -> Result<ManifestFile> {
        let table = snapshot_produce.table;

        let new_manifest_path = format!(
            "{}/metadata/{}-m-overwrite.avro",
            table.metadata().location(),
            Uuid::now_v7(),
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            manifest_file.key_metadata.clone(),
            table.metadata().current_schema().clone(),
            table
                .metadata()
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Partition spec {} not found in table metadata",
                            manifest_file.partition_spec_id
                        ),
                    )
                })?
                .as_ref()
                .clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        for entry in manifest.entries() {
            if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
                let mut deleted: ManifestEntry = (**entry).clone();
                deleted.snapshot_id = Some(self.snapshot_id);
                writer.add_deleted_entry(deleted)?;
            } else {
                let cloned: ManifestEntry = (**entry).clone();
                writer.add_existing_entry(cloned)?;
            }
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, ManifestStatus,
        Operation, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn test_data_file(path: &str, partition_spec_id: i32) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_empty_data_overwrite_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.overwrite().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = test_data_file(
            "test/1.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx
            .overwrite()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_overwrite_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = tx.overwrite().add_data_files(vec![data_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = test_data_file(
            "test/3.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx.overwrite().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );

        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_overwrite_with_deleted_files() {
        use crate::memory::tests::new_memory_catalog;
        use crate::transaction::ApplyTransactionAction;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file = test_data_file("test/original.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());

        let replacement_file = test_data_file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(2, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted
                    && path == "test/original.parquet"),
            "Original file should be marked as Deleted, entries: {all_entries:?}",
        );

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Added
                    && path == "test/replacement.parquet"),
            "Replacement file should be marked as Added, entries: {all_entries:?}",
        );

        // Verify snapshot summary reports the deleted file.
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .map(|s| s.as_str()),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-records")
                .map(|s| s.as_str()),
            Some("1")
        );

        // Step 3: Fast append after overwrite — delete-only manifest must survive.
        let appended_file = test_data_file("test/appended.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![appended_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // 3 manifests: rewritten (deleted entry), overwrite added, fast_append added.
        assert_eq!(3, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        // The deleted entry must still be present after fast_append.
        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted
                    && path == "test/original.parquet"),
            "Deleted entry should survive fast_append, entries: {all_entries:?}",
        );
    }

    #[tokio::test]
    async fn test_overwrite_unaffected_manifest_passthrough() {
        use crate::memory::tests::new_memory_catalog;
        use crate::transaction::ApplyTransactionAction;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Commit file A in its own manifest.
        let file_a = test_data_file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Commit file B in a second manifest.
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Record the manifest paths before the overwrite.
        let pre_snapshot = table.metadata().current_snapshot().unwrap();
        let pre_manifest_list = pre_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(2, pre_manifest_list.entries().len());

        // Find which manifest contains file B (the unaffected one).
        let mut manifest_b_path = None;
        for mf in pre_manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            if manifest
                .entries()
                .iter()
                .any(|e| e.file_path() == "test/b.parquet")
            {
                manifest_b_path = Some(mf.manifest_path.clone());
            }
        }
        let manifest_b_path = manifest_b_path.expect("manifest for file B not found");

        // Overwrite: delete file A only, no adds (delete-only overwrite).
        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_a.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let post_snapshot = table.metadata().current_snapshot().unwrap();
        let post_manifest_list = post_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should still have 2 manifests: one rewritten (A deleted), one passed through (B unchanged).
        assert_eq!(
            2,
            post_manifest_list.entries().len(),
            "Expected 2 manifests after delete-only overwrite, got {}",
            post_manifest_list.entries().len()
        );

        // The manifest containing file B should be the same path (passthrough).
        let passthrough = post_manifest_list
            .entries()
            .iter()
            .find(|mf| mf.manifest_path == manifest_b_path);
        assert!(
            passthrough.is_some(),
            "Manifest for file B should be passed through unchanged, post manifests: {:?}",
            post_manifest_list
                .entries()
                .iter()
                .map(|e| &e.manifest_path)
                .collect::<Vec<_>>()
        );

        // File A should be marked deleted, file B should still be alive.
        let mut all_entries = vec![];
        for mf in post_manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }
        assert!(
            all_entries
                .iter()
                .any(|(s, p)| *s == ManifestStatus::Deleted && p == "test/a.parquet"),
            "File A should be Deleted, entries: {all_entries:?}"
        );
        assert!(
            all_entries
                .iter()
                .any(|(s, p)| *s != ManifestStatus::Deleted && p == "test/b.parquet"),
            "File B should still be alive, entries: {all_entries:?}"
        );
    }

    #[tokio::test]
    async fn test_delete_only_overwrite_summary() {
        use crate::memory::tests::new_memory_catalog;
        use crate::transaction::ApplyTransactionAction;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        // Append 3 files.
        let file1 = test_data_file("test/f1.parquet", spec_id);
        let file2 = test_data_file("test/f2.parquet", spec_id);
        let file3 = test_data_file("test/f3.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action =
            tx.fast_append()
                .add_data_files(vec![file1.clone(), file2.clone(), file3.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Delete one file with a delete-only overwrite (no adds).
        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let props = &snapshot.summary().additional_properties;

        // Overwrite semantics use truncate_table_summary which treats overwrite as
        // "replace all": deleted-data-files reflects the previous total (3), not the
        // number of explicitly deleted files (1). Without Fix 1, previous_snapshot
        // would be None so previous_total=0 and u64 subtraction would underflow.
        assert_eq!(
            props.get("deleted-data-files").map(|s| s.as_str()),
            Some("3"),
            "Expected deleted-data-files=3 (overwrite semantics), got: {props:?}"
        );

        // total-data-files should be 0 after a delete-only overwrite (overwrite
        // semantics: 3 removed - 3 previous = 0). Without Fix 1 this would panic
        // with u64 underflow in debug mode.
        assert_eq!(
            props.get("total-data-files").map(|s| s.as_str()),
            Some("0"),
            "Expected total-data-files=0 (overwrite semantics), got: {props:?}"
        );
    }
}
