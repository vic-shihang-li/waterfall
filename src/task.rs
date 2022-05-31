use dashmap::mapref::one::{Ref as DashMapRef, RefMut as DashMapRefMut};
use dashmap::{DashMap, DashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TaskId(String);

impl TaskId {
    fn new() -> Self {
        TaskId(Uuid::new_v4().to_string())
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

    pub fn new_task(&mut self, task_name: &str) -> NewTaskHandle {
        self._create_task(task_name.into(), None)
    }

    pub fn new_task_with_description(
        &mut self,
        task_name: &str,
        description: &str,
    ) -> NewTaskHandle {
        self._create_task(task_name.into(), Some(description.into()))
    }

    fn _create_task(&mut self, name: String, description: Option<String>) -> NewTaskHandle {
        let t = Task::new(name, description, self.inner.clone());
        let id_for_handle = t.id.clone();
        let id = t.id.clone();
        self.inner.insert(id, t);
        NewTaskHandle::new(id_for_handle, self.inner.clone())
    }

    pub fn get(&self, id: &TaskId) -> Option<TaskRef<'_>> {
        self.inner.get(id).map(TaskRef::from)
    }

    pub fn get_mut(&self, id: &TaskId) -> Option<TaskRefMut<'_>> {
        self.inner.get_mut(id).map(TaskRefMut::from)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AddDependencyError {
    CycleDetected,
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
    deps: Option<DashSet<TaskId>>,
}

impl Task {
    fn new(task_name: String, description: Option<String>, tasks: Arc<TaskMap>) -> Self {
        Self {
            tasks,
            id: TaskId::new(),
            name: task_name,
            description,
            deps: None,
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
        match &self.deps {
            None => false,
            Some(deps) => !deps.is_empty(),
        }
    }

    fn num_dependencies(&self) -> usize {
        match &self.deps {
            None => 0,
            Some(deps) => deps.len(),
        }
    }

    fn depends_on(&self, target: &Task) -> Option<DependencyKind> {
        if self.deps.as_ref()?.contains(&target.id) {
            return Some(DependencyKind::Direct);
        }

        for dep_id in self.deps.as_ref().unwrap().iter() {
            let dep = self.tasks.get(&dep_id).expect("Found dangling task");
            if dep.depends_on(target).is_some() {
                return Some(DependencyKind::Transitive);
            }
        }

        None
    }

    fn add_dependency(&mut self, dependency: &Task) -> Result<(), AddDependencyError> {
        if dependency.depends_on(self).is_some() {
            return Err(AddDependencyError::CycleDetected);
        }

        if self.deps.is_none() {
            self.deps = Some(DashSet::new());
        }
        self.deps.as_mut().unwrap().insert(dependency.id.clone());

        Ok(())
    }
}

mod tests {
    use super::*;

    #[test]
    fn create_task_with_name() {
        let mut mngr = TaskManager::new();
        let handle = mngr.new_task("hello world!");
        let t = handle.get().unwrap();
        assert_eq!(t.name(), "hello world!");
    }

    #[test]
    fn create_task_with_description() {
        let mut mngr = TaskManager::new();
        let name = "hello world!";
        let description = "this is a very small task";
        let handle = mngr.new_task_with_description(name, description);
        let t = handle.get().unwrap();
        assert_eq!(t.description().unwrap(), description);
    }

    #[test]
    fn update_task() {
        let mut mngr = TaskManager::new();
        let handle = mngr.new_task("hi");
        let mut t = handle.get_mut().unwrap();

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
        let handle = mngr.new_task("hello world");
        let mut t = handle.get_mut().unwrap();
        assert!(!t.completed());

        t.complete();
        assert!(t.completed());

        t.complete();
        assert!(t.completed());
    }

    #[test]
    fn create_task_with_dependency() {
        let mut mngr = TaskManager::new();

        let handle = mngr.new_task("dependent task");
        let dep = handle.get().unwrap();

        let handle = mngr.new_task("parent");
        let mut parent = handle.get_mut().unwrap();

        parent.add_dependency(&dep).unwrap();

        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
        assert_eq!(parent.depends_on(&dep).unwrap(), DependencyKind::Direct);
    }

    #[test]
    fn create_transitive_dependency() {
        // t1 -> t2 -> t3
        
        let mut mngr = TaskManager::new();

        let handle1 = mngr.new_task("t1");
        let handle2 = mngr.new_task("t2");
        let handle3 = mngr.new_task("t3");

        let mut t1 = handle1.get_mut_unchecked();
        let mut t2 = handle2.get_mut_unchecked();
        let mut t3 = handle3.get_mut_unchecked();

        t1.add_dependency(&t2).unwrap();
        t2.add_dependency(&t3).unwrap();
        assert_eq!(t1.depends_on(&t3).unwrap(), DependencyKind::Transitive);
    }

    #[test]
    fn dependency_is_unidirectional() {
        // t1 -> t2 -> t3
        
        let mut mngr = TaskManager::new();

        let handle1 = mngr.new_task("t1");
        let handle2 = mngr.new_task("t2");
        let handle3 = mngr.new_task("t3");

        let mut t1 = handle1.get_mut_unchecked();
        let mut t2 = handle2.get_mut_unchecked();
        let mut t3 = handle3.get_mut_unchecked();

        t1.add_dependency(&t2).unwrap();
        t2.add_dependency(&t3).unwrap();
        assert!(t1.depends_on(&t3).is_some());
        assert!(t3.depends_on(&t1).is_none());
    }

    #[test]
    fn prevent_duplicate_dependencies() {
        let mut mngr = TaskManager::new();

        let handle = mngr.new_task("dependent");
        let dep = handle.get().unwrap();
        let handle = mngr.new_task("parent");
        let mut parent = handle.get_mut().unwrap();

        for _ in 0..100 {
            parent.add_dependency(&dep).unwrap();
        }

        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
    }

    #[test]
    fn prevent_simple_cycle() {
        // t1 -> t2 -> t1

        let mut mngr = TaskManager::new();

        let handle1 = mngr.new_task("t1");
        let handle2 = mngr.new_task("t2");

        let mut t1 = handle1.get_mut_unchecked();
        let mut t2 = handle2.get_mut_unchecked();

        assert!(t1.add_dependency(&t2).is_ok());
        assert_eq!(
            t2.add_dependency(&t1).err(),
            Some(AddDependencyError::CycleDetected)
        );
    }

    #[test]
    fn prevent_linear_dependency_cycle() {
        // t1 -> t2 -> t3 -> t4 -> t1

        let mut mngr = TaskManager::new();

        let handle1 = mngr.new_task("t1");
        let handle2 = mngr.new_task("t2");
        let handle3 = mngr.new_task("t3");
        let handle4 = mngr.new_task("t4");

        let mut t1 = handle1.get_mut_unchecked();
        let mut t2 = handle2.get_mut_unchecked();
        let mut t3 = handle3.get_mut_unchecked();
        let mut t4 = handle4.get_mut_unchecked();

        assert!(t1.add_dependency(&t2).is_ok());
        assert!(t2.add_dependency(&t3).is_ok());
        assert!(t3.add_dependency(&t4).is_ok());
        assert_eq!(
            t4.add_dependency(&t1).err(),
            Some(AddDependencyError::CycleDetected)
        );
    }
}
