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
pub enum AddDependencyError {
    CycleDetected,
    TaskNotFound(TaskId),
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

        for dep_id in self.deps.iter() {
            let dep = self.tasks.get(&dep_id).expect("Found dangling task");
            if dep.depends_on(target).is_some() {
                return Some(DependencyKind::Transitive);
            }
        }

        None
    }

    fn add_dependency(&self, dependency_id: &TaskId) -> Result<(), AddDependencyError> {
        match self.tasks.get(dependency_id) {
            None => Err(AddDependencyError::TaskNotFound(*dependency_id)),
            Some(dep) => {
                if dep.depends_on(&self.id).is_some() {
                    return Err(AddDependencyError::CycleDetected);
                }
                self.deps.insert(dep.id.clone());
                Ok(())
            }
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn create_task_with_name() {
        let mut mngr = TaskManager::new();
        let id = mngr.new_task("hello world!");
        let t = mngr.get(&id).unwrap();
        assert_eq!(t.name(), "hello world!");
    }

    #[test]
    fn create_task_with_description() {
        let mut mngr = TaskManager::new();
        let name = "hello world!";
        let description = "this is a very small task";
        let id = mngr.new_task_with_description(name, description);
        let t = mngr.get(&id).unwrap();
        assert_eq!(t.description().unwrap(), description);
    }

    #[test]
    fn update_task() {
        let mut mngr = TaskManager::new();
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
        let mut mngr = TaskManager::new();
        let id = mngr.new_task("hello world");
        let mut t = mngr.get_mut(&id).unwrap();
        assert!(!t.completed());

        t.complete();
        assert!(t.completed());

        t.complete();
        assert!(t.completed());
    }

    #[test]
    fn create_task_with_dependency() {
        let mut mngr = TaskManager::new();

        let dep_id = mngr.new_task("dependent task");

        let parent_id = mngr.new_task("parent");
        let mut parent = mngr.get_mut(&parent_id).unwrap();

        parent.add_dependency(&dep_id).unwrap();

        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
        assert_eq!(parent.depends_on(&dep_id).unwrap(), DependencyKind::Direct);
    }

    #[test]
    fn create_transitive_dependency() {
        // t1 -> t2 -> t3

        let mut mngr = TaskManager::new();

        let id1 = mngr.new_task("t1");
        let id2 = mngr.new_task("t2");
        let id3 = mngr.new_task("t3");

        mngr.get_mut(&id1).unwrap().add_dependency(&id2).unwrap();
        mngr.get_mut(&id2).unwrap().add_dependency(&id3).unwrap();
        assert_eq!(
            mngr.get(&id1).unwrap().depends_on(&id3).unwrap(),
            DependencyKind::Transitive
        );
    }

    #[test]
    fn dependency_is_unidirectional() {
        // t1 -> t2 -> t3

        let mut mngr = TaskManager::new();

        let id1 = mngr.new_task("t1");
        let id2 = mngr.new_task("t2");
        let id3 = mngr.new_task("t3");

        mngr.get_mut(&id1).unwrap().add_dependency(&id2).unwrap();
        mngr.get_mut(&id2).unwrap().add_dependency(&id3).unwrap();

        assert!(mngr.get(&id1).unwrap().depends_on(&id3).is_some());
        assert!(mngr.get(&id3).unwrap().depends_on(&id1).is_none());
    }

    #[test]
    fn prevent_duplicate_dependencies() {
        let mut mngr = TaskManager::new();

        let dep_id = mngr.new_task("dependent");
        let parent_id = mngr.new_task("parent");

        for _ in 0..100 {
            mngr.get_mut(&parent_id)
                .unwrap()
                .add_dependency(&dep_id)
                .unwrap();
        }

        let parent = mngr.get(&parent_id).unwrap();
        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
    }

    #[test]
    fn prevent_simple_cycle() {
        // t1 -> t2 -> t1

        let mut mngr = TaskManager::new();

        let id1 = mngr.new_task("t1");
        let id2 = mngr.new_task("t2");

        assert!(mngr.get_mut(&id1).unwrap().add_dependency(&id2).is_ok());
        assert_eq!(
            mngr.get_mut(&id2).unwrap().add_dependency(&id1).err(),
            Some(AddDependencyError::CycleDetected)
        );
    }

    #[test]
    fn prevent_linear_dependency_cycle() {
        // t1 -> t2 -> t3 -> t4 -> t1

        let mut mngr = TaskManager::new();

        let id1 = mngr.new_task("t1");
        let id2 = mngr.new_task("t2");
        let id3 = mngr.new_task("t3");
        let id4 = mngr.new_task("t4");

        assert!(mngr.get_mut(&id1).unwrap().add_dependency(&id2).is_ok());
        assert!(mngr.get_mut(&id2).unwrap().add_dependency(&id3).is_ok());
        assert!(mngr.get_mut(&id3).unwrap().add_dependency(&id4).is_ok());
        assert_eq!(
            mngr.get_mut(&id4).unwrap().add_dependency(&id1).err(),
            Some(AddDependencyError::CycleDetected)
        );
    }
}
