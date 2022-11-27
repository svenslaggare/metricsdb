use std::collections::{HashMap, HashSet};
use std::fmt::{Display};
use std::path::{Path, PathBuf};
use fnv::FnvHashSet;

use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{Error, Visitor};

use crate::model::{MetricError, MetricResult, Tags};

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Tag(pub String, pub String);

impl Tag {
    pub fn from_ref(key: &str, value: &str) -> Tag {
        Tag(key.to_owned(), value.to_owned())
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

struct TagVisitor;
impl<'de> Visitor<'de> for TagVisitor {
    type Value = Tag;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string on the format key:value")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> where E: Error {
        self.visit_str(&value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where E: Error {
        let parts = value.split(":").collect::<Vec<_>>();
        if parts.len() == 2 {
            Ok(Tag(parts[0].to_owned(), parts[1].to_owned()))
        } else {
            Err(E::custom("string on the format key:value"))
        }
    }
}

impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(deserializer: D) -> Result<Tag, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_string(TagVisitor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PrimaryTag {
    Default,
    Named(Tag)
}

impl PrimaryTag {
    pub fn named(&self) -> Option<&Tag> {
        match self {
            PrimaryTag::Default => None,
            PrimaryTag::Named(tag) => Some(tag)
        }
    }

    pub fn path(&self, base_path: &Path) -> PathBuf {
        match &self {
            PrimaryTag::Default => base_path.join("default"),
            PrimaryTag::Named(tag) => base_path.join(&tag.to_string())
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum TagsFilter {
    None,
    And(Vec<Tag>),
    Or(Vec<Tag>),
    OrAnd(Vec<Tag>, Vec<Tag>)
}

impl TagsFilter {
    pub fn apply(&self,
                 named_primary_tags: &HashSet<&Tag>,
                 primary_tag: &PrimaryTag,
                 tags_index: &SecondaryTagsIndex) -> Option<SecondaryTagsFilter> {
        fn remove_tag<'a>(tags: &'a Vec<Tag>, primary_tag: &'a Tag) -> impl Iterator<Item=&'a Tag> {
            tags.iter().filter(move |tag| *tag != primary_tag)
        }

        let contains_any_named_primary_tag = |tags: &Vec<Tag>| {
            for tag in tags {
                if named_primary_tags.contains(tag) {
                    return true;
                }
            }

            false
        };

        match self {
            TagsFilter::None => Some(SecondaryTagsFilter::None),
            TagsFilter::And(tags) => {
                match primary_tag {
                    PrimaryTag::Named(primary_tag) => {
                        if tags.contains(primary_tag) {
                            Some(SecondaryTagsFilter::And(tags_index.tags_pattern(remove_tag(tags, primary_tag))?))
                        } else if contains_any_named_primary_tag(tags) {
                            None
                        } else {
                            Some(SecondaryTagsFilter::And(tags_index.tags_pattern(tags.iter())?))
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
                            Some(SecondaryTagsFilter::Or(tags_index.tags_pattern(tags.iter())?))
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

    pub fn add_and_clause(self, mut tags: Vec<Tag>) -> TagsFilter {
        match self {
            TagsFilter::None => TagsFilter::And(tags),
            TagsFilter::And(mut current) => {
                current.append(&mut tags);
                TagsFilter::And(current)
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
    mapping: HashMap<Tag, Tags>,
    all_patterns: FnvHashSet<Tags>,
    #[serde(skip)]
    tags_pattern_to_string: HashMap<Tags, Tag>
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

    pub fn try_add_tags(&mut self, tags: &[Tag]) -> MetricResult<Tags> {
        let mut changed = false;
        for tag in tags {
            changed |= self.try_add(tag).ok_or_else(|| MetricError::ExceededSecondaryTags)?.1;
        }

        if changed {
            self.save()?;
        }

        let pattern = self.tags_pattern(tags.iter()).ok_or_else(|| MetricError::ExceededSecondaryTags)?;
        self.all_patterns.insert(pattern);
        Ok(pattern)
    }

    pub fn try_add(&mut self, tag: &Tag) -> Option<(Tags, bool)> {
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

    pub fn tags_pattern<'a>(&'a self, tags: impl Iterator<Item=&'a Tag>) -> Option<Tags> {
        let mut pattern = 0;
        for tag in tags {
            pattern |= self.mapping.get(tag)?;
        }

        Some(pattern)
    }

    pub fn tags_pattern_to_string(&self, tags: &Tags) -> Option<&Tag> {
        self.tags_pattern_to_string.get(tags)
    }

    pub fn all_patterns(&self) -> &FnvHashSet<Tags> {
        &self.all_patterns
    }

    pub fn save(&self) -> MetricResult<()> {
        let save = || {
            let content = serde_json::to_string(&self)?;
            std::fs::write(&self.base_path.join("tags.json"), &content)?;
            Ok(())
        };

        save().map_err(|err| MetricError::FailedToSavePrimaryTag(err))?;
        Ok(())
    }

    pub fn load(path: &Path) -> MetricResult<SecondaryTagsIndex> {
        let load = || {
            let content = std::fs::read_to_string(path)?;
            let mut tags: SecondaryTagsIndex = serde_json::from_str(&content)?;

            for (tag, tag_pattern) in tags.mapping.iter() {
                tags.tags_pattern_to_string.insert(*tag_pattern, tag.to_owned());
            }

            Ok(tags)
        };

        load().map_err(|err| MetricError::FailedToLoadSecondaryTag(err))
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
fn serialize_tag1() {
    let tag = Tag("host".to_owned(), "Test".to_owned());
    let output = serde_json::to_string(&tag).unwrap();
    assert_eq!("\"host:Test\"", output);
    assert_eq!(tag, serde_json::from_str::<Tag>(&output).unwrap());
}

#[test]
fn test_try_add1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    for number in 1..(Tags::BITS + 1) {
        assert_eq!(true, index.try_add(&Tag("tag".to_owned(), format!("T{}", number))).is_some());
        assert_eq!(true, index.try_add(&Tag("tag".to_owned(), format!("T{}", number))).is_some());
    }

    assert_eq!(true, index.try_add(&Tag("tag".to_owned(), format!("T{}", 33))).is_some());
    assert_eq!(true, index.try_add(&Tag("tag".to_owned(), format!("T{}", Tags::BITS + 1))).is_none());
}

#[test]
fn test_and_filter1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&Tag::from_ref("tag", "T1")).unwrap();
    index.try_add(&Tag::from_ref("tag", "T2")).unwrap();

    assert_eq!(Some(SecondaryTagsFilter::And(1)), index.tags_pattern([Tag::from_ref("tag", "T1")].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
    assert_eq!(Some(SecondaryTagsFilter::And(1 | 2)), index.tags_pattern([Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
}

#[test]
fn test_and_filter2() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&Tag::from_ref("tag", "T1")).unwrap();
    index.try_add(&Tag::from_ref("tag", "T2")).unwrap();

    assert_eq!(None, index.tags_pattern([Tag::from_ref("tag", "T3"), Tag::from_ref("tag", "T1")].iter()).map(|pattern| SecondaryTagsFilter::And(pattern)));
}

#[test]
fn test_or_filter1() {
    let mut index = SecondaryTagsIndex::new(Path::new(""));
    index.try_add(&Tag::from_ref("tag", "T1")).unwrap();
    index.try_add(&Tag::from_ref("tag", "T2")).unwrap();

    assert_eq!(Some(SecondaryTagsFilter::Or(1 | 2)), index.tags_pattern([Tag::from_ref("tag", "T1"), Tag::from_ref("tag", "T2")].iter()).map(|pattern| SecondaryTagsFilter::Or(pattern)));
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

#[test]
fn test_primary_tags_filter1() {
    let tags_filter = TagsFilter::And(vec![Tag::from_ref("t1", "v1"), Tag::from_ref("t2", "v1")]);
    let mut tags_index = SecondaryTagsIndex::new(Path::new("dummy"));
    let pattern = tags_index.try_add(&Tag::from_ref("t2", "v1")).unwrap().0;
    let mut primary_tags = HashSet::new();
    let tag = Tag::from_ref("t1", "v1");
    primary_tags.insert(&tag);

    assert_eq!(
        Some(SecondaryTagsFilter::And(pattern)),
        tags_filter.apply(&primary_tags, &PrimaryTag::Named(Tag::from_ref("t1", "v1")), &tags_index)
    )
}

#[test]
fn test_primary_tags_filter2() {
    let tags_filter = TagsFilter::And(vec![Tag::from_ref("t2", "v1")]);
    let mut tags_index = SecondaryTagsIndex::new(Path::new("dummy"));
    let pattern = tags_index.try_add(&Tag::from_ref("t2", "v1")).unwrap().0;
    let mut primary_tags = HashSet::new();
    let tag = Tag::from_ref("t1", "v1");
    primary_tags.insert(&tag);

    assert_eq!(
        Some(SecondaryTagsFilter::And(pattern)),
        tags_filter.apply(&primary_tags, &PrimaryTag::Named(Tag::from_ref("t1", "v1")), &tags_index)
    )
}

#[test]
fn test_primary_tags_filter3() {
    let tag1 = Tag::from_ref("t2", "v1");
    let tag2 = Tag::from_ref("t1", "v1");
    let tag3 = Tag::from_ref("t1", "v2");

    let tags_filter = TagsFilter::And(vec![Tag::from_ref("t1", "v1"), Tag::from_ref("t2", "v1")]);
    let mut tags_index = SecondaryTagsIndex::new(Path::new("dummy"));
    tags_index.try_add(&tag1).unwrap();
    let mut primary_tags = HashSet::new();
    primary_tags.insert(&tag2);
    primary_tags.insert(&tag3);

    assert_eq!(
        None,
        tags_filter.apply(&primary_tags, &PrimaryTag::Named(Tag::from_ref("t1", "v2")), &tags_index)
    )
}
