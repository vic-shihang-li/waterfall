use std::collections::HashSet;
use uuid::Uuid;

pub struct Task {
    id: String,
    name: String,
    description: Option<String>,
    completed: bool,
    depends_on: Option<HashSet<String>>,
}

impl Task {
    pub fn new(task_name: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: task_name,
            description: None,
            depends_on: None,
            completed: false,
        }
    }

    pub fn with_description(task_name: String, description: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: task_name,
            description: Some(description),
            depends_on: None,
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
        match &self.depends_on {
            None => false,
            Some(deps) => !deps.is_empty(),
        }
    }

    fn num_dependencies(&self) -> usize {
        match &self.depends_on {
            None => 0,
            Some(deps) => deps.len(),
        }
    }

    fn add_dependency(&mut self, dependency: &Task) {
        if self.depends_on.is_none() {
            self.depends_on = Some(HashSet::from([dependency.id.clone()]));
        } else {
            self.depends_on
                .as_mut()
                .unwrap()
                .insert(dependency.id.clone());
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn create_task_with_name() {
        let name = "hello world!";
        let t = Task::new(name.to_string());
        assert_eq!(t.name(), name);
    }

    #[test]
    fn create_task_with_description() {
        let name = "hello world!";
        let description = "this is a very small task";
        let t = Task::with_description(name.to_string(), description.to_string());
        assert_eq!(t.description().unwrap(), description);
    }

    #[test]
    fn update_task() {
        let mut t = Task::new("hi".to_string());

        let new_name = "new name";
        let new_desc = "new description";

        t.update_name(new_name.to_string());
        assert_eq!(t.name(), new_name);

        t.update_description(new_desc.to_string());
        assert_eq!(t.description().unwrap(), new_desc);
    }

    #[test]
    fn complete_task() {
        let mut t = Task::new("hello world".to_string());
        assert!(!t.completed());

        t.complete();
        assert!(t.completed());

        t.complete();
        assert!(t.completed());
    }

    #[test]
    fn create_task_with_dependency() {
        let dep = Task::new(String::from("dependent task"));
        let mut parent = Task::new(String::from("parent"));
        parent.add_dependency(&dep);

        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
    }

    #[test]
    fn prevent_duplicate_dependencies() {
        let dep = Task::new(String::from("dependent task"));
        let mut parent = Task::new(String::from("parent"));

        for _ in 0..100 {
            parent.add_dependency(&dep);
        }

        assert!(parent.has_dependencies());
        assert_eq!(parent.num_dependencies(), 1);
    }
}
