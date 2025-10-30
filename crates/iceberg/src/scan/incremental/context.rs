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

use std::collections::HashSet;
use std::sync::Arc;

use futures::channel::mpsc::Sender;

use crate::Result;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::delete_file_index::DeleteFileIndex;
use crate::io::object_cache::ObjectCache;
use crate::scan::ExpressionEvaluatorCache;
use crate::scan::context::{ManifestEntryContext, ManifestEntryFilterFn, ManifestFileContext};
use crate::spec::{
    ManifestContentType, ManifestEntryRef, ManifestFile, Operation, SchemaRef, SnapshotRef,
    TableMetadataRef,
};

#[derive(Debug)]
pub(crate) struct IncrementalPlanContext {
    /// The snapshots involved in the incremental scan.
    pub snapshots: Vec<SnapshotRef>,

    /// The snapshot to start the incremental scan from.
    pub from_snapshot: SnapshotRef,

    /// The metadata of the table being scanned.
    pub table_metadata: TableMetadataRef,

    /// The schema of the snapshot to end the incremental scan at.
    pub to_snapshot_schema: SchemaRef,

    /// The object cache to use for the scan.
    pub object_cache: Arc<ObjectCache>,

    /// The field IDs to scan.
    pub field_ids: Arc<Vec<i32>>,

    /// The expression evaluator cache to use for the scan.
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,

    /// The caching delete file loader to use for the scan.
    pub caching_delete_file_loader: CachingDeleteFileLoader,
}

impl IncrementalPlanContext {
    pub(crate) async fn build_manifest_file_contexts(
        &self,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx: DeleteFileIndex,
        delete_file_tx: Sender<ManifestEntryContext>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        // Validate that all snapshots are Append or Delete operations and collect their IDs
        let snapshot_ids: HashSet<i64> = {
            let mut ids = HashSet::new();
            for snapshot in self.snapshots.iter() {
                let operation = &snapshot.summary().operation;
                if !matches!(
                    operation,
                    Operation::Append | Operation::Overwrite | Operation::Delete
                ) {
                    return Err(crate::Error::new(
                        crate::ErrorKind::FeatureUnsupported,
                        format!(
                            "Incremental scan only supports Append, Overwrite and Delete operations, but snapshot {} has operation {:?}",
                            snapshot.snapshot_id(),
                            operation
                        ),
                    ));
                }
                ids.insert(snapshot.snapshot_id());
            }
            ids
        };

        let (manifest_files, filter_fn) = {
            let mut manifest_files = HashSet::<ManifestFile>::new();
            for snapshot in self.snapshots.iter() {
                let manifest_list = self
                    .object_cache
                    .get_manifest_list(snapshot, &self.table_metadata)
                    .await?;
                for entry in manifest_list.entries() {
                    if !snapshot_ids.contains(&entry.added_snapshot_id) {
                        continue;
                    }
                    manifest_files.insert(entry.clone());
                }
            }
            let filter_fn: Option<Arc<ManifestEntryFilterFn>> =
                Some(Arc::new(move |entry: &ManifestEntryRef| {
                    entry
                        .snapshot_id()
                        .map(|id| snapshot_ids.contains(&id))
                        .unwrap_or(true) // Include entries without `snapshot_id`.
                }));

            (manifest_files, filter_fn)
        };

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut mfcs = vec![];
        for manifest_file in &manifest_files {
            let tx = if manifest_file.content == ManifestContentType::Deletes {
                delete_file_tx.clone()
            } else {
                tx_data.clone()
            };

            let mfc = ManifestFileContext {
                manifest_file: manifest_file.clone(),
                bound_predicates: None,
                sender: tx,
                object_cache: self.object_cache.clone(),
                snapshot_schema: self.to_snapshot_schema.clone(),
                field_ids: self.field_ids.clone(),
                expression_evaluator_cache: self.expression_evaluator_cache.clone(),
                delete_file_index: delete_file_idx.clone(),
                filter_fn: filter_fn.clone(),
            };

            mfcs.push(Ok(mfc));
        }

        Ok(Box::new(mfcs.into_iter()))
    }
}
