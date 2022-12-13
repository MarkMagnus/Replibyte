use crate::transformer::Transformer;
use crate::types::Column;

/// This struct is dedicated to generating random elements.
pub struct BlankTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl BlankTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
        where
            S: Into<String>,
    {
        BlankTransformer {
            table_name: table_name.into(),
            column_name: column_name.into(),
            database_name: database_name.into(),
        }
    }
}

impl Default for BlankTransformer {
    fn default() -> Self {
        BlankTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl Transformer for BlankTransformer {
    fn id(&self) -> &str {
        "blank"
    }

    fn description(&self) -> &str { "blank/nil value completely" }

    fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn column_name(&self) -> &str {
        self.column_name.as_str()
    }

    fn transform(&self, column: Column) -> Column {
        Column::None(self.column_name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer, transformer::Transformer, types::Column};

    use super::{BlankTransformer};

    #[test]
    fn transform_anything_to_nil() {
        let transformer = BlankTransformer::new("github", "user", "merge_attributes");
        let column = Column::StringValue("merge_attributes".to_string(), "signup_source => Admin, australian => False, created_at => 06/10/2022".to_string());
        let transformed_column = transformer.transform(column);

        assert!(matches!(transformed_column, Column::None{..}));
    }
}