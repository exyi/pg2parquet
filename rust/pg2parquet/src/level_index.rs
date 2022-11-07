use std::fmt::Debug;


pub struct LevelIndexList<'a> {
	pub index: usize,
	pub level: i16,
	pub parent: Option<&'a LevelIndexList<'a>>
}

impl<'a> LevelIndexList<'a> {
	pub fn new_i(index: usize) -> Self {
		LevelIndexList {
			index,
			level: 0,
			parent: None
		}
	}
	pub fn new() -> Self {
		Self::new_i(0)
	}

	pub fn new_child<'b: 'a>(&'b self) -> LevelIndexList<'b> {
		LevelIndexList {
			index: 0,
			level: self.level + 1,
			parent: Some(&self)
		}
	}

	pub fn inc(&mut self) {
		self.index += 1;
	}
}
impl<'a> Debug for LevelIndexList<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut s = LevelIndexState::new(self.level);
		s.copy_and_diff(self);
		write!(f, "lvl_l{:?}", s.indexes)
	}
}

#[derive(Clone)]
pub struct LevelIndexState {
	pub indexes: Vec<usize>
}

impl Debug for LevelIndexState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "lvl_s{:?}", self.indexes)
	}
}

impl LevelIndexState {
	pub fn new(level: i16) -> Self {
		LevelIndexState {
			indexes: vec![usize::MAX; (level + 1) as usize]
		}
	}

	pub fn level(&self) -> i16 {
		(self.indexes.len() - 1) as i16
	}

	
	pub fn inc(&mut self, lvl: i16) {
		self.indexes[lvl as usize] += 1;
		for i in (lvl + 1) as usize..self.indexes.len() {
			self.indexes[i] = 0;
		}
	}

	pub fn copy_and_diff(&mut self, other: &LevelIndexList) -> i16 {
		debug_assert_eq!(self.level(), other.level);

		let mut result = other.level;
		
		let mut current_lvl = other;
		let mut i = other.level;
		loop {

			debug_assert_eq!(i, current_lvl.level);

			if self.indexes[i as usize] != current_lvl.index {
				self.indexes[i as usize] = current_lvl.index;
				result = i;
			}


			match current_lvl.parent {
				Some(parent) => {
					current_lvl = parent;
					i -= 1;
				},
				None => break
			}
		}

		result
	}

	pub fn copy_from(&mut self, other: &LevelIndexState) {
		debug_assert_eq!(self.indexes.len(), other.indexes.len());

		self.indexes.copy_from_slice(other.indexes.as_slice());
	}

	pub fn get_level_difference(&self, other: &LevelIndexState) -> i16 {
		for i in 0..self.indexes.len() {
			if self.indexes[i] != other.indexes[i] {
				return i as i16;
			}
		}
		return self.indexes.len() as i16;
	}
}
