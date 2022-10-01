use std::collections::HashMap;

use crate::{Tags};

pub struct TagsIndex {
    mapping: HashMap<String, Tags>
}

impl TagsIndex {
    pub fn new() -> TagsIndex {
        TagsIndex {
            mapping: HashMap::new()
        }
    }

    pub fn try_add(&mut self, tag: &str) -> Option<Tags> {
        if let Some(pattern) = self.mapping.get(tag) {
            return Some(*pattern);
        } else if self.mapping.len() < 64 {
            let pattern = 1 << self.mapping.len();
            self.mapping.insert(tag.to_owned(), pattern);
            Some(pattern)
        } else {
            None
        }
    }

    pub fn tags_pattern(&self, tags: &[&str]) -> Option<Tags> {
        let mut pattern = 0;
        for tag in tags {
            pattern |= self.mapping.get(*tag)?;
        }

        Some(pattern)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagsFilter {
    None,
    And(Tags),
    Or(Tags)
}

impl TagsFilter {
    pub fn accept(&self, tags: Tags) -> bool {
        match self {
            TagsFilter::None => true,
            TagsFilter::And(pattern) => (tags & pattern) == *pattern,
            TagsFilter::Or(pattern) => (tags & pattern) != 0
        }
    }
}

#[test]
fn test_try_add1() {
    let mut index = TagsIndex::new();
    for number in 1..65 {
        assert_eq!(true, index.try_add(&format!("tag:T{}", number)).is_some());
        assert_eq!(true, index.try_add(&format!("tag:T{}", number)).is_some());
    }

    assert_eq!(true, index.try_add(&format!("tag:T{}", 33)).is_some());
    assert_eq!(true, index.try_add(&format!("tag:T{}", 65)).is_none());
}

#[test]
fn test_and_filter1() {
    let mut index = TagsIndex::new();
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(Some(TagsFilter::And(1)), index.tags_pattern(&["tag:T1"]).map(|pattern| TagsFilter::And(pattern)));
    assert_eq!(Some(TagsFilter::And(1 | 2)), index.tags_pattern(&["tag:T1", "tag:T2"]).map(|pattern| TagsFilter::And(pattern)));
}

#[test]
fn test_and_filter2() {
    let mut index = TagsIndex::new();
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(None, index.tags_pattern(&["tag:T3", "tag:T1"]).map(|pattern| TagsFilter::And(pattern)));
}

#[test]
fn test_or_filter1() {
    let mut index = TagsIndex::new();
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(Some(TagsFilter::Or(1 | 2)), index.tags_pattern(&["tag:T1", "tag:T2"]).map(|pattern| TagsFilter::Or(pattern)));
}