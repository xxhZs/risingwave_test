// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::util::sort_util::OrderType;

use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::{TableCatalog, TableId};
use crate::optimizer::property::{Direction, FieldOrder};

#[derive(Default)]
pub struct TableCatalogBuilder {
    columns: Vec<ColumnCatalog>,
    column_names: HashMap<String, i32>,
    order_key: Vec<FieldOrder>,
    pk_indices: Vec<usize>,
}

/// For DRY, mainly used for construct internal table catalog in stateful streaming executors.
/// Be careful of the order of add column.
impl TableCatalogBuilder {
    // TODO: Add more fields if internal table is more configurable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column from Field info, return the column index of the table
    pub fn add_column(&mut self, field: &Field) -> usize {
        let column_idx = self.columns.len();
        let column_id = column_idx as i32;
        // Add column desc.
        let mut column_desc = ColumnDesc::from_field_with_column_id(field, column_id);

        // Avoid column name duplicate.
        self.avoid_duplicate_col_name(&mut column_desc);

        self.columns.push(ColumnCatalog {
            column_desc: column_desc.clone(),
            // All columns in internal table are invisible to batch query.
            is_hidden: false,
        });
        column_idx
    }

    /// Check whether need to add a ordered column. Different from value, order desc equal pk in
    /// semantics and they are encoded as storage key.
    pub fn add_order_column(&mut self, index: usize, order_type: OrderType) {
        self.pk_indices.push(index);
        self.order_key.push(FieldOrder {
            index,
            direct: match order_type {
                OrderType::Ascending => Direction::Asc,
                OrderType::Descending => Direction::Desc,
            },
        });
    }

    /// Check the column name whether exist before. if true, record occurrence and change the name
    /// to avoid duplicate.
    fn avoid_duplicate_col_name(&mut self, column_desc: &mut ColumnDesc) {
        let column_name = column_desc.name.clone();
        if let Some(occurrence) = self.column_names.get_mut(&column_name) {
            column_desc.name = format!("{}_{}", column_name, occurrence);
            *occurrence += 1;
        } else {
            self.column_names.insert(column_name, 0);
        }
    }

    /// Consume builder and create `TableCatalog` (for proto).
    pub fn build(self, distribution_key: Vec<usize>, append_only: bool) -> TableCatalog {
        TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name: String::new(),
            columns: self.columns,
            order_key: self.order_key,
            pk: self.pk_indices,
            is_index_on: None,
            distribution_key,
            appendonly: append_only,
            owner: risingwave_common::catalog::DEFAULT_SUPPER_USER.to_string(),
            vnode_mapping: None,
            properties: HashMap::default(),
            read_pattern_prefix_column: 0,
        }
    }
}
