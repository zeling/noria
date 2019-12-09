use std::collections::HashMap;

use crate::prelude::*;

/// Guard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Guard {
    us: Option<IndexPair>,
    src: IndexPair,
    user_column: Option<usize>,

    // Index for user in persistent storage for objection.
    key_column: usize,
    // Index for Objection value in persistent storage.
    value_column: usize,
}

impl Guard {
    /// Construct a new Guard operator.
    ///
    /// `src` is the parent node from which this node receives records.
    ///  Specifically, it will be a base node.
    pub fn new(src: NodeIndex) -> Guard {
        Guard {
            us: None,
            src: src.into(),
            user_column: None,
            key_column: 0,
            value_column: 1,
        }
    }
}

impl Ingredient for Guard {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self, detailed: bool) -> String {
        // More information, when detailed = true.
        String::from("G")
    }

    fn on_connected(&mut self, graph: &Graph) {
        let srcn = &graph[self.src.as_global()];
        // Only connect to base node.
        assert!(srcn.is_base());

        self.user_column = srcn.get_base().unwrap().user_column.clone();
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        _: LocalNodeIndex,
        mut rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
        _: &DomainNodes,
        states: &StateMap,
    ) -> ProcessingResult {
        if let Some(ucolumn) = self.user_column {
            // Find the current permission for each user.
            // TODO: create persistent state when creating this operator.
            let us = self.us.unwrap().as_local().unwrap();
            let db = states
                .get(us)
                .expect("guard must have its own persistent state for permission.");

            rs.retain(|r| {
                match db.lookup(&[self.key_column], &KeyType::Single(&r[ucolumn])) {
                    LookupResult::Some(RecordResult::Owned(rows)) => {
                        match rows.len() {
                            0 => true, // TODO: should prompt the user to provide her objection intention.
                            1 => rows[0][self.value_column] == 0.into(), // here we specify 1 as objected and 0 as not objected.
                            _ => unreachable!(), // normally, should be only one record for objection
                        }
                    }
                    _ => unreachable!(),
                }
            });
        }
        ProcessingResult {
            results: rs,
            ..Default::default()
        }
    }

    fn can_query_through(&self) -> bool {
        false
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base_with_user_column("source", &["user", "age"], Some(0));
        g.set_op(
            "guard",
            &["user", "age"],
            Guard::new(s.as_global()),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_without_guard() {
        let mut g = setup(false);

        // setup persistent state.
        let state = PersistentState::new(
            String::from("setup guard testcase(unused)"),
            Some(&[0]),
            &PersistenceParameters::default(),
        );
        g.set_persistent_state(state);

        let mut left: Vec<DataType>;

        left = vec!["Alice".into(), 10.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec!["Bob".into(), 12.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_with_guard() {
        let mut g = setup(false);

        // setup persistent state.
        let mut state = PersistentState::new(
            String::from("setup guard testcase(used)"),
            Some(&[0]),
            &PersistenceParameters {
                mode: DurabilityMode::Permanent,
                ..PersistenceParameters::default()
            },
        );
        let mut guard_records: Records = vec![
            (vec!["Alice".into(), 1.into()], true), // Alice objects.
            (vec!["Bob".into(), 0.into()], true),   // Bob does not object.
        ]
        .into();
        state.process_records(&mut guard_records, None);
        g.set_persistent_state(state);

        let mut left: Vec<DataType>;

        // Alice objects.
        left = vec!["Alice".into(), 10.into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // Bob does not object.
        left = vec!["Bob".into(), 12.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // John does not specify and defaults to not object.
        left = vec!["John".into(), 15.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }
}
