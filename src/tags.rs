use std::collections::HashMap;

use crate::{Tags, TagsFilter};

pub struct TagsIndex {
    mapping: HashMap<String, Tags>
}

impl TagsIndex {
    pub fn new() -> TagsIndex {
        TagsIndex {
            mapping: HashMap::new()
        }
    }

    pub fn try_add(&mut self, tag: String) -> Option<Tags> {
        if let Some(pattern) = self.mapping.get(&tag) {
            return Some(*pattern);
        } else if self.mapping.len() < 64 {
            let pattern = 1 << self.mapping.len();
            self.mapping.insert(tag, pattern);
            Some(pattern)
        } else {
            None
        }
    }

    pub fn tags_pattern(&self, tags: &[String]) -> Option<Tags> {
        let mut pattern = 0;
        for tag in tags {
            pattern |= self.mapping.get(tag)?;
        }

        Some(pattern)
    }

    pub fn and_filter(&self, tags: &[String]) -> Option<TagsFilter> {
        Some(TagsFilter::And(self.tags_pattern(tags)?))
    }

    pub fn or_filter(&self, tags: &[String]) -> Option<TagsFilter> {
        Some(TagsFilter::Or(self.tags_pattern(tags)?))
    }
}

#[test]
fn test_try_add1() {
    let mut index = TagsIndex::new();
    for number in 1..65 {
        assert_eq!(true, index.try_add(format!("tag:T{}", number)).is_some());
        assert_eq!(true, index.try_add(format!("tag:T{}", number)).is_some());
    }

    assert_eq!(true, index.try_add(format!("tag:T{}", 33)).is_some());
    assert_eq!(true, index.try_add(format!("tag:T{}", 65)).is_none());
}

#[test]
fn test_and_filter1() {
    let mut index = TagsIndex::new();
    index.try_add("tag:T1".to_string()).unwrap();
    index.try_add("tag:T2".to_string()).unwrap();

    assert_eq!(Some(TagsFilter::And(1)), index.and_filter(&["tag:T1".to_owned()]));
    assert_eq!(Some(TagsFilter::And(1 | 2)), index.and_filter(&["tag:T1".to_owned(), "tag:T2".to_owned()]));
}

#[test]
fn test_and_filter2() {
    let mut index = TagsIndex::new();
    index.try_add("tag:T1".to_string()).unwrap();
    index.try_add("tag:T2".to_string()).unwrap();

    assert_eq!(None, index.and_filter(&["tag:T3".to_owned(), "tag:T1".to_owned()]));
}

#[test]
fn test_or_filter1() {
    let mut index = TagsIndex::new();
    index.try_add("tag:T1".to_string()).unwrap();
    index.try_add("tag:T2".to_string()).unwrap();

    assert_eq!(Some(TagsFilter::Or(1 | 2)), index.or_filter(&["tag:T1".to_owned(), "tag:T2".to_owned()]));
}