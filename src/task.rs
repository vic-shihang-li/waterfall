use dashmap::mapref::one::{Ref as DashMapRef, RefMut as DashMapRefMut};
use dashmap::{DashMap, DashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);

impl TaskId {
    fn new() -> Self {
        TaskId(NEXT_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

type TaskMap = DashMap<TaskId, Task>;

pub struct TaskRef<'a> {
    inner: DashMapRef<'a, TaskId, Task>,
}

impl<'a> From<DashMapRef<'a, TaskId, Task>> for TaskRef<'a> {
    fn from(actual_ref: DashMapRef<'a, TaskId, Task>) -> Self {
        Self { inner: actual_ref }
    }
}

impl<'a> Deref for TaskRef<'a> {
    type Target = Task;

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

pub struct TaskRefMut<'a> {
    inner: DashMapRefMut<'a, TaskId, Task>,
}

impl<'a> From<DashMapRefMut<'a, TaskId, Task>> for TaskRefMut<'a> {
    fn from(actual_ref: DashMapRefMut<'a, TaskId, Task>) -> Self {
        Self { inner: actual_ref }
    }
}

impl<'a> Deref for TaskRefMut<'a> {
    type Target = Task;

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

impl<'a> DerefMut for TaskRefMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.value_mut()
    }
}

pub struct NewTaskHandle {
    id: TaskId,
    map: Arc<TaskMap>,
}

impl NewTaskHandle {
    fn new(id: TaskId, map: Arc<TaskMap>) -> Self {
        Self { id, map }
    }

    fn get(&self) -> Option<TaskRef<'_>> {
        self.map.get(&self.id).map(TaskRef::from)
    }

    fn get_unchecked(&self) -> TaskRef<'_> {
        self.get().unwrap()
    }

    fn get_mut(&self) -> Option<TaskRefMut<'_>> {
        self.map.get_mut(&self.id).map(TaskRefMut::from)
    }

    fn get_mut_unchecked(&self) -> TaskRefMut<'_> {
        self.get_mut().unwrap()
    }
}

pub struct TaskManager {
    inner: Arc<TaskMap>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_task(&self, task_name: &str) -> TaskId {
        self._create_task(task_name.into(), None)
    }

    pub fn new_task_with_description(&self, task_name: &str, description: &str) -> TaskId {
        self._create_task(task_name.into(), Some(description.into()))
    }

    fn _create_task(&self, name: String, description: Option<String>) -> TaskId {
        let t = Task::new(name, description, self.inner.clone());
        let id = t.id;
        self.inner.insert(id, t);
        id
    }

    pub fn get(&self, id: &TaskId) -> Option<TaskRef<'_>> {
        self.inner.get(id).map(TaskRef::from)
    }

    pub fn get_mut(&self, id: &TaskId) -> Option<TaskRefMut<'_>> {
        self.inner.get_mut(id).map(TaskRefMut::from)
    }

    pub fn add_dependency(
        &self,
        parent: &TaskId,
        child: &TaskId,
    ) -> Result<(), AddDependencyError> {
        match self.inner.get(parent) {
            None => Err(AddDependencyError::TaskNotFound(*parent)),
            Some(parent_task) => parent_task.add_dependency(child),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Dependency {
    from: TaskId,
    to: TaskId,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AddDependencyError {
    CycleDetected(Dependency),
    TaskNotFound(TaskId),
}

macro_rules! err_dep_cycle {
    ($from_task_id: expr, $to_task_id: expr) => {{
        AddDependencyError::CycleDetected(Dependency {
            from: $from_task_id,
            to: $to_task_id,
        })
    }};
}

#[derive(Debug, PartialEq, Eq)]
enum DependencyKind {
    Direct,
    Transitive,
}

pub struct Task {
    tasks: Arc<TaskMap>,
    id: TaskId,
    name: String,
    description: Option<String>,
    completed: bool,
    deps: DashSet<TaskId>,
}

impl Task {
    fn new(task_name: String, description: Option<String>, tasks: Arc<TaskMap>) -> Self {
        Self {
            tasks,
            id: TaskId::new(),
            name: task_name,
            description,
            deps: DashSet::new(),
            completed: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn update_name(&mut self, new_name: String) {
        self.name = new_name;
    }

    pub fn update_description(&mut self, new_description: String) {
        self.description = Some(new_description);
    }

    pub fn description(&self) -> Option<&str> {
        match &self.description {
            None => None,
            Some(d) => Some(d),
        }
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn complete(&mut self) {
        self.completed = true;
    }

    fn has_dependencies(&self) -> bool {
        !self.deps.is_empty()
    }

    fn num_dependencies(&self) -> usize {
        self.deps.len()
    }

    fn depends_on(&self, target: &TaskId) -> Option<DependencyKind> {
        if self.deps.contains(target) {
            return Some(DependencyKind::Direct);
        }

        let mut buf1: Vec<TaskId> = vec![self.id];
        let mut buf2: Vec<TaskId> = vec![];
        let mut iter = 0;
        loop {
            let (curr_row, next_row) = {
                if iter % 2 == 0 {
                    (&buf1, &mut buf2)
                } else {
                    (&buf2, &mut buf1)
                }
            };

            if curr_row.is_empty() {
                return None;
            }

            next_row.clear();
            for curr_id in curr_row {
                let curr = self.tasks.get(curr_id).unwrap();
                for dep_id in curr.deps.iter() {
                    if *dep_id == *target {
                        return Some(DependencyKind::Transitive);
                    }
                    next_row.push(*dep_id);
                }
            }

            iter += 1;
        }
    }

    fn add_dependency(&self, dependency_id: &TaskId) -> Result<(), AddDependencyError> {
        match self.tasks.get(dependency_id) {
            None => Err(AddDependencyError::TaskNotFound(*dependency_id)),
            Some(dep) => {
                if dep.depends_on(&self.id).is_some() {
                    return Err(err_dep_cycle!(self.id, dep.id));
                }
                self.deps.insert(dep.id);
                Ok(())
            }
        }
    }
}

mod tests {
    use super::*;

    impl TaskManager {
        fn create_random_tasks(&self, num_tasks: usize) -> Vec<TaskId> {
            (0..num_tasks)
                .map(|_| self.new_task("random_task"))
                .collect()
        }

        fn add_dependency_chain_from_ids(
            &self,
            task_ids: &[TaskId],
        ) -> Result<(), AddDependencyError> {
            task_ids
                .iter()
                .zip(task_ids.iter().skip(1))
                .try_for_each(|(parent, child)| self.add_dependency(parent, child))
        }

        fn add_dependency_chain_from_refs(
            &self,
            task_ids: &[&TaskId],
        ) -> Result<(), AddDependencyError> {
            task_ids
                .iter()
                .zip(task_ids.iter().skip(1))
                .try_for_each(|(parent, child)| self.add_dependency(parent, child))
        }
    }

    mod basic {
        use super::*;

        #[test]
        fn create_task_with_name() {
            let manager = TaskManager::new();
            let id = manager.new_task("hello world!");
            let t = manager.get(&id).unwrap();
            assert_eq!(t.name(), "hello world!");
        }

        #[test]
        fn create_task_with_description() {
            let manager = TaskManager::new();
            let name = "hello world!";
            let description = "this is a very small task";
            let id = manager.new_task_with_description(name, description);
            let t = manager.get(&id).unwrap();
            assert_eq!(t.description().unwrap(), description);
        }

        #[test]
        fn update_task() {
            let mngr = TaskManager::new();
            let id = mngr.new_task("hi");
            let mut t = mngr.get_mut(&id).unwrap();

            let new_name = "new name";
            let new_desc = "new description";

            t.update_name(new_name.to_string());
            assert_eq!(t.name(), new_name);

            t.update_description(new_desc.to_string());
            assert_eq!(t.description().unwrap(), new_desc);
        }

        #[test]
        fn complete_task() {
            let manager = TaskManager::new();
            let id = manager.new_task("hello world");
            let mut t = manager.get_mut(&id).unwrap();
            assert!(!t.completed());

            t.complete();
            assert!(t.completed());

            t.complete();
            assert!(t.completed());
        }
    }

    mod add_deps {
        use rand::Rng;

        use super::*;

        #[test]
        fn create_task_with_dependency() {
            let manager = TaskManager::new();

            let dep_id = manager.new_task("dependent task");
            let parent_id = manager.new_task("parent");

            manager.add_dependency(&parent_id, &dep_id).unwrap();

            let parent = manager.get(&parent_id).unwrap();
            assert!(parent.has_dependencies());
            assert_eq!(parent.num_dependencies(), 1);
            assert_eq!(parent.depends_on(&dep_id).unwrap(), DependencyKind::Direct);
        }

        #[test]
        fn create_transitive_dependency() {
            // t1 -> t2 -> t3

            let manager = TaskManager::new();

            let id1 = manager.new_task("t1");
            let id2 = manager.new_task("t2");
            let id3 = manager.new_task("t3");

            manager.add_dependency(&id1, &id2).unwrap();
            manager.add_dependency(&id2, &id3).unwrap();

            assert_eq!(
                manager.get(&id1).unwrap().depends_on(&id3).unwrap(),
                DependencyKind::Transitive
            );
        }

        #[test]
        fn dependency_is_unidirectional() {
            // t1 -> t2 -> t3

            let manager = TaskManager::new();

            let id1 = manager.new_task("t1");
            let id2 = manager.new_task("t2");
            let id3 = manager.new_task("t3");

            manager.add_dependency(&id1, &id2).unwrap();
            manager.add_dependency(&id2, &id3).unwrap();

            assert!(manager.get(&id1).unwrap().depends_on(&id3).is_some());
            assert!(manager.get(&id3).unwrap().depends_on(&id1).is_none());
        }

        #[test]
        fn long_dependency_chain() {
            let manager = TaskManager::new();

            let ids = manager.create_random_tasks(1_000);

            manager
                .add_dependency_chain_from_ids(ids.as_slice())
                .unwrap();

            let mut count = 0;
            let target = 1000;

            while count < target {
                let id1 = rand::thread_rng().gen_range(0..ids.len());
                let id2 = rand::thread_rng().gen_range(0..ids.len());
                if id1 == id2 {
                    continue;
                }

                let parent = if id1 < id2 { id1 } else { id2 };
                let child = if id1 < id2 { id2 } else { id1 };

                assert!(manager
                    .get(&ids[parent])
                    .unwrap()
                    .depends_on(&ids[child])
                    .is_some());
                count += 1;
            }
        }

        #[test]
        fn prevent_duplicate_dependencies() {
            let manager = TaskManager::new();

            let dep_id = manager.new_task("dependent");
            let parent_id = manager.new_task("parent");

            for _ in 0..100 {
                manager.add_dependency(&parent_id, &dep_id).unwrap();
            }

            let parent = manager.get(&parent_id).unwrap();
            assert!(parent.has_dependencies());
            assert_eq!(parent.num_dependencies(), 1);
        }
    }

    mod no_cyclic_deps {
        use super::*;

        #[test]
        fn prevent_simple_cycle() {
            // t1 -> t2 -> t1

            let manager = TaskManager::new();

            let id1 = manager.new_task("t1");
            let id2 = manager.new_task("t2");

            assert!(manager.add_dependency(&id1, &id2).is_ok());
            assert_eq!(
                manager.add_dependency(&id2, &id1).err(),
                Some(err_dep_cycle!(id2, id1))
            );
        }

        #[test]
        fn prevent_linear_dependency_cycle() {
            // t1 -> t2 -> t3 -> t4 -> t1

            let manager = TaskManager::new();

            let id1 = manager.new_task("t1");
            let id2 = manager.new_task("t2");
            let id3 = manager.new_task("t3");
            let id4 = manager.new_task("t4");

            assert!(manager.add_dependency(&id1, &id2).is_ok());
            assert!(manager.add_dependency(&id2, &id3).is_ok());
            assert!(manager.add_dependency(&id3, &id4).is_ok());
            assert_eq!(
                manager.add_dependency(&id4, &id1).err(),
                Some(err_dep_cycle!(id4, id1))
            );
        }

        #[test]
        fn prevent_cycles_with_multiple_paths() {
            // t1 -> t2 -> t3 -> t4
            //  `--- t5 <---^

            let manager = TaskManager::new();

            match manager.create_random_tasks(5).as_slice() {
                [id1, id2, id3, id4, id5] => {
                    manager
                        .add_dependency_chain_from_refs(&[id1, id2, id3, id4])
                        .unwrap();

                    assert_eq!(
                        manager
                            .add_dependency_chain_from_refs(&[id3, id5, id1])
                            .err(),
                        Some(err_dep_cycle!(*id5, *id1))
                    );
                }
                _ => unreachable!(),
            }
        }
    }
}
