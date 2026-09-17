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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod expire_snapshots;
mod overwrite;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_schema;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};
use serde_derive::{Deserialize, Serialize};
pub use update_schema::AddColumn;

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
pub use crate::transaction::append::FastAppendAction;
pub use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
pub use crate::transaction::overwrite::OverwriteAction;
pub use crate::transaction::sort_order::ReplaceSortOrderAction;
pub use crate::transaction::update_location::UpdateLocationAction;
pub use crate::transaction::update_properties::UpdatePropertiesAction;
pub use crate::transaction::update_schema::UpdateSchemaAction;
pub use crate::transaction::update_statistics::UpdateStatisticsAction;
pub use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
}

/// The result of [`Transaction::stage_commit`]: a table (or one element of a multi-table
/// transaction) commit that has been fully computed but not yet submitted to a catalog.
///
/// Serializes to exactly the JSON body the Iceberg REST catalog protocol expects for
/// `POST .../tables/{table}` (this shape, with `identifier` omitted since the URL already
/// names the table) or as one element of `POST .../transactions/commit`'s `table-changes`
/// array (with `identifier` present, as it is here).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StagedCommit {
    /// The table this commit applies to.
    pub identifier: TableIdent,
    /// Requirements that must hold for this commit to be accepted.
    pub requirements: Vec<TableRequirement>,
    /// Metadata changes to apply once the requirements are validated.
    pub updates: Vec<TableUpdate>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
        }
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();
        let unchecked_requirements = action_commit.take_unchecked_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);
        existing_requirements.extend(unchecked_requirements);

        Ok(updated_table)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates an update schema action.
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates an overwrite action.
    pub fn overwrite(&self) -> OverwriteAction {
        OverwriteAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Expire snapshots from the table metadata.
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Runs every action's commit logic — writing any new manifest/manifest-list files to
    /// storage as a side effect, via the table's existing `FileIO` — and returns the
    /// resulting identifier, requirements and updates without submitting them to the
    /// catalog. The caller is responsible for eventually sending this as a commit, to this
    /// catalog or to whatever external component finalizes it; [`StagedCommit`] serializes
    /// to exactly the JSON body the Iceberg REST catalog protocol expects for a table or
    /// transaction commit.
    ///
    /// Unlike [`Transaction::commit`], this never reads current catalog state and never
    /// retries — the returned requirements assert against exactly the base each action
    /// resolved from the `Table` this `Transaction` was constructed with (or from any
    /// override supplied via `OverwriteAction::assert_requirements`).
    pub async fn stage_commit(self) -> Result<StagedCommit> {
        let identifier = self.table.identifier().to_owned();
        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        Ok(StagedCommit {
            identifier,
            requirements: existing_requirements,
            updates: existing_updates,
        })
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props = self.table.metadata().table_properties();

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties<'_>) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms()?))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms()?))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms()?,
            )))
            .with_max_times(props.commit_num_retries()?)
            .with_factor(2.0)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Operation, Struct,
        TableMetadata, TableProperties,
    };
    use crate::table::Table;
    use crate::test_utils::{make_encrypted_table, test_runtime};
    use crate::transaction::{ApplyTransactionAction, StagedCommit, Transaction};
    use crate::{
        Catalog, Error, ErrorKind, TableCommit, TableCreation, TableIdent, TableRequirement,
    };

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table()
            .times(expected_calls)
            .returning_st(move |_| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }

    #[tokio::test]
    async fn test_transaction_snapshot_summary() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let mut file_seq = 0u32;
        let mut append_file = |table: &Table, record_count: u64, file_size: u64| {
            file_seq += 1;
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{file_seq}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(1))]))
                .partition_spec_id(0)
                .build()
                .unwrap();
            let tx = Transaction::new(table);
            tx.fast_append()
                .add_data_files(vec![file])
                .apply(tx)
                .unwrap()
        };

        let table = append_file(&table, /*record_count=*/ 10, /*file_size=*/ 100)
            .commit(&catalog)
            .await
            .unwrap();
        let table = append_file(&table, /*record_count=*/ 20, /*file_size=*/ 200)
            .commit(&catalog)
            .await
            .unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;

        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        assert_eq!(summary.get("total-files-size").unwrap(), "300");
    }

    fn make_stage_commit_data_file(table: &Table, path: &str) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_stage_commit_does_not_touch_catalog() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_file = make_stage_commit_data_file(&table, "test/staged-1.parquet");

        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite()
            .add_data_files(vec![data_file])
            .apply(tx)
            .unwrap();

        let staged = tx.stage_commit().await.unwrap();
        assert_eq!(staged.identifier, *table.identifier());
        assert!(!staged.requirements.is_empty());
        assert!(!staged.updates.is_empty());

        // Nothing was submitted -- reloading from the catalog shows the table unchanged.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(reloaded.metadata(), table.metadata());
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            table.metadata().current_snapshot_id()
        );
    }

    #[tokio::test]
    async fn test_stage_commit_payload_is_a_valid_commit() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_file = make_stage_commit_data_file(&table, "test/staged-2.parquet");

        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite()
            .add_data_files(vec![data_file])
            .apply(tx)
            .unwrap();

        let mut staged = tx.stage_commit().await.unwrap();

        // Submit the staged payload directly, bypassing `Transaction::commit` entirely --
        // proving `StagedCommit` alone (identifier + requirements + updates) is a complete,
        // usable commit, exactly as an external committer would submit it.
        let table_commit = TableCommit::builder()
            .ident(staged.identifier.clone())
            .requirements(std::mem::take(&mut staged.requirements))
            .updates(std::mem::take(&mut staged.updates))
            .build();
        let committed = catalog.update_table(table_commit).await.unwrap();

        assert!(committed.metadata().current_snapshot().is_some());
        assert_eq!(
            committed
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );

        // And the catalog now durably reflects it.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            committed.metadata().current_snapshot_id()
        );
    }

    #[tokio::test]
    async fn test_stage_commit_respects_assert_requirements_override() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_file = make_stage_commit_data_file(&table, "test/staged-3.parquet");

        let overridden_base = table
            .metadata()
            .current_snapshot_id()
            .map(|id| id + 1)
            .unwrap_or(1);

        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite()
            .add_data_files(vec![data_file])
            .assert_requirements(vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(overridden_base),
            }])
            .apply(tx)
            .unwrap();

        let staged = tx.stage_commit().await.unwrap();
        assert!(
            staged
                .requirements
                .contains(&TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: Some(overridden_base),
                })
        );
    }

    #[tokio::test]
    async fn test_stage_commit_serializes_to_rest_catalog_shape() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_file = make_stage_commit_data_file(&table, "test/staged-4.parquet");

        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite()
            .add_data_files(vec![data_file])
            .apply(tx)
            .unwrap();

        let staged = tx.stage_commit().await.unwrap();
        let json = serde_json::to_value(&staged).unwrap();

        assert!(json.get("identifier").is_some());
        let requirements = json["requirements"].as_array().unwrap();
        assert!(
            requirements
                .iter()
                .any(|r| r["type"] == "assert-ref-snapshot-id")
        );
        let updates = json["updates"].as_array().unwrap();
        assert!(updates.iter().any(|u| u["action"] == "add-snapshot"));
        assert!(updates.iter().any(|u| u["action"] == "set-snapshot-ref"));

        // Round-trips.
        let round_tripped: StagedCommit = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, staged);
    }

    #[tokio::test]
    async fn test_commit_to_encrypted_table() {
        let table = make_encrypted_table().await.with_metadata_location(
            "memory:///table/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json"
                .to_string(),
        );
        let refreshed_table = table.clone();
        let update_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_load_table()
            .times(1)
            .returning_st(move |_| {
                let refreshed_table = refreshed_table.clone();
                Box::pin(async move { Ok(refreshed_table) })
            });
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let update_table = update_table.clone();
                Box::pin(async move { commit.apply(update_table) })
            });

        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap();

        let updated_table = tx.commit(&mock_catalog).await.unwrap();

        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .map(String::as_str),
            Some("master-1")
        );
        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );
        assert!(updated_table.encryption_manager().is_some());
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
