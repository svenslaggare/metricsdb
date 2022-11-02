use std::collections::{HashMap};
use std::path::{Path, PathBuf};
use fnv::FnvHashSet;

use serde::{Serialize, Deserialize};

use crate::model::{MetricError, MetricResult, Tags};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PrimaryTag {
    Default,
    Named(String)
}

#[derive(Debug, Clone)]
pub enum TagsFilter {
    None,
    And(Vec<String>),
    Or(Vec<String>),
    OrAnd(Vec<String>, Vec<String>)
}

impl TagsFilter {
    pub fn apply(&self, tags_index: &SecondaryTagsIndex, primary_tag: &PrimaryTag) -> Option<SecondaryTagsFilter> {
        fn remove_tag<'a>(tags: &'a Vec<String>, primary_tag: &'a str) -> impl Iterator<Item=&'a String> {
            tags.iter().filter(move |tag| *tag != primary_tag)
        }

        match self {
            TagsFilter::None => Some(SecondaryTagsFilter::None),
            TagsFilter::And(tags) => {
                match primary_tag {
                    PrimaryTag::Named(primary_tag) => {
                        if tags.contains(primary_tag) {
                            Some(SecondaryTagsFilter::And(tags_index.tags_pattern(remove_tag(tags, primary_tag))?))
                        } else {
                            None
                        }
                    }
                    PrimaryTag::Default => {
                        Some(SecondaryTagsFilter::And(tags_index.tags_pattern(tags.iter())?))
                    }
                }
            }
            TagsFilter::Or(tags) => {
                match primary_tag {
                    PrimaryTag::Named(primary_tag) => {
                        if tags.contains(primary_tag) {
                            Some(SecondaryTagsFilter::None)
                        } else {
                            Some(SecondaryTagsFilter::Or(tags_index.tags_pattern(remove_tag(tags, primary_tag))?))
                        }
                    }
                    PrimaryTag::Default => {
                        Some(SecondaryTagsFilter::Or(tags_index.tags_pattern(tags.iter())?))
                    }
                }
            }
            TagsFilter::OrAnd(left, right) => {
                match primary_tag {
                    PrimaryTag::Named(primary_tag) => {
                        if left.contains(primary_tag) {
                            Some(SecondaryTagsFilter::Or(tags_index.tags_pattern(right.iter())?))
                        } else if right.contains(primary_tag) {
                            Some(SecondaryTagsFilter::Or(tags_index.tags_pattern(left.iter())?))
                        } else {
                            Some(
                                SecondaryTagsFilter::OrAnd(
                                    tags_index.tags_pattern(remove_tag(left, primary_tag))?,
                                    tags_index.tags_pattern(remove_tag(right, primary_tag))?
                                )
                            )
                        }
                    }
                    PrimaryTag::Default => {
                        Some(
                            SecondaryTagsFilter::OrAnd(
                                tags_index.tags_pattern(left.iter())?,
                                tags_index.tags_pattern(right.iter())?
                            )
                        )
                    }
                }
            }
        }
    }

    pub fn add_and_clause(self, mut tags: Vec<String>) -> TagsFilter {
        match self {
            TagsFilter::None => TagsFilter::And(tags),
            TagsFilter::And(mut current) => {
                current.append(&mut tags);
                TagsFilter::Or(current)
            }
            TagsFilter::Or(current) => {
                TagsFilter::OrAnd(current, tags)
            }
            TagsFilter::OrAnd(_, _) => {
                unimplemented!("Not supported.");
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SecondaryTagsIndex {
    base_path: PathBuf,
    mapping: HashMap<String, Tags>,
    all_patterns: FnvHashSet<Tags>,
    #[serde(skip)]
    tags_pattern_to_string: HashMap<Tags, String>
}

impl SecondaryTagsIndex {
    pub fn new(base_path: &Path) -> SecondaryTagsIndex {
        SecondaryTagsIndex {
            base_path: base_path.to_owned(),
            mapping: HashMap::new(),
            all_patterns: FnvHashSet::default(),
            tags_pattern_to_string: HashMap::new()
        }
    }

    pub fn try_add_tags(&mut self, tags: &[String]) -> MetricResult<Tags> {
        let mut changed = false;
        for tag in tags {
            changed |= self.try_add(tag).ok_or_else(|| MetricError::ExceededSecondaryTags)?.1;
        }

        if changed {
            self.save().map_err(|err| MetricError::FailedToSaveSecondaryTag(err))?;
        }

        let pattern = self.tags_pattern(tags.iter()).ok_or_else(|| MetricError::ExceededSecondaryTags)?;
        self.all_patterns.insert(pattern);
        Ok(pattern)
    }

    pub fn try_add(&mut self, tag: &str) -> Option<(Tags, bool)> {
        if let Some(pattern) = self.mapping.get(tag) {
            return Some((*pattern, false));
        } else if self.mapping.len() < Tags::BITS as usize {
            let pattern = 1 << self.mapping.len() as Tags;
            let inserted = self.mapping.insert(tag.to_owned(), pattern).is_none();
            if inserted {
                self.tags_pattern_to_string.insert(pattern, tag.to_owned());
            }

            Some((pattern, inserted))
        } else {
            None
        }
    }

    pub fn tags_pattern<T: AsRef<str>>(&self, tags: impl Iterator<Item=T>) -> Option<Tags> {
        let mut pattern = 0;
        for tag in tags {
            pattern |= self.mapping.get(tag.as_ref())?;
        }

        Some(pattern)
    }

    pub fn tags_pattern_to_string(&self, tags: &Tags) -> Option<&String> {
        self.tags_pattern_to_string.get(tags)
    }

    pub fn all_patterns(&self) -> &FnvHashSet<Tags> {
        &self.all_patterns
    }

    pub fn save(&self) -> std::io::Result<()> {
        let content = serde_json::to_string(&self)?;
        std::fs::write(&self.base_path.join("tags.json"), &content)?;
        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<SecondaryTagsIndex> {
        let content = std::fs::read_to_string(path)?;
        let mut tags: SecondaryTagsIndex = serde_json::from_str(&content)?;

        for (tag, tag_pattern) in tags.mapping.iter() {
            tags.tags_pattern_to_string.insert(*tag_pattern, tag.to_owned());
        }

        Ok(tags)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryTagsFilter {
    None,
    And(Tags),
    Or(Tags),
    OrAnd(Tags, Tags)
}

impl SecondaryTagsFilter {
    pub fn accept(&self, tags: Tags) -> bool {
        match self {
            SecondaryTagsFilter::None => true,
            SecondaryTagsFilter::And(pattern) => (tags & pattern) == *pattern,
            SecondaryTagsFilter::Or(pattern) => (tags & pattern) != 0,
            SecondaryTagsFilter::OrAnd(left, right) => ((tags & left) != 0) && ((tags & right) != 0)
        }
    }
}

#[test]
fn test_try_add1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    for number in 1..(Tags::BITS + 1) {
        assert_eq!(true, index.try_add(&format!("tag:T{}", number)).is_some());
        assert_eq!(true, index.try_add(&format!("tag:T{}", number)).is_some());
    }

    assert_eq!(true, index.try_add(&format!("tag:T{}", 33)).is_some());
    assert_eq!(true, index.try_add(&format!("tag:T{}", Tags::BITS + 1)).is_none());
}

#[test]
fn test_and_filter1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(Some(SecondaryTagsFilter::And(1)), index.tags_pattern(["tag:T1".to_owned()].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
    assert_eq!(Some(SecondaryTagsFilter::And(1 | 2)), index.tags_pattern(["tag:T1".to_owned(), "tag:T2".to_owned()].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
}

#[test]
fn test_and_filter2() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(None, index.tags_pattern(["tag:T3".to_owned(), "tag:T1".to_owned()].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
}

#[test]
fn test_or_filter1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&"tag:T1".to_string()).unwrap();
    index.try_add(&"tag:T2".to_string()).unwrap();

    assert_eq!(Some(SecondaryTagsFilter::Or(1 | 2)), index.tags_pattern(["tag:T1".to_owned(), "tag:T2".to_owned()].iter()).map(|pattern| SecondaryTagsFilter::Or(pattern)));
}

#[test]
fn test_tags_filter1() {
    let current_tags = 0;
    assert_eq!(false, SecondaryTagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter2() {
    let current_tags = 1;
    assert_eq!(true, SecondaryTagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter3() {
    let current_tags = 1 | (1 << 2);
    assert_eq!(true, SecondaryTagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter4() {
    let current_tags = 1;
    assert_eq!(false, SecondaryTagsFilter::And(1 | (1 << 2)).accept(current_tags));
}

#[test]
fn test_tags_filter5() {
    let current_tags = 1;
    assert_eq!(true, SecondaryTagsFilter::Or(1).accept(current_tags));
}

#[test]
fn test_tags_filter6() {
    let current_tags = 1;
    assert_eq!(true, SecondaryTagsFilter::Or(1 | (1 << 2)).accept(current_tags));
}

#[test]
fn test_tags_filter7() {
    let current_tags = 1 | (1 << 2);
    assert_eq!(true, SecondaryTagsFilter::Or(1).accept(current_tags));
}

#[test]
fn test_tags_filter8() {
    let current_tags = 2;
    assert_eq!(false, SecondaryTagsFilter::Or(1).accept(current_tags));
}

#[test]
fn test_tags_filter9() {
    let current_tags = 1 | 2;
    assert_eq!(true, SecondaryTagsFilter::OrAnd(1, 2).accept(current_tags));
}

#[test]
fn test_tags_filter10() {
    let current_tags = 1;
    assert_eq!(false, SecondaryTagsFilter::OrAnd(1, 2).accept(current_tags));
}