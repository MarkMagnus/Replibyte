pub mod Hstore {

    use std::collections::HashMap;
    use crate::source::clean_quotes;

    pub fn from_hstore(s: String) -> HashMap<String, String> {
        let mut kv = HashMap::new();
        let clean_string = clean_quotes(s);
        for values in clean_string.split("\", \"") {
            let elements: Vec<&str> = values.split("\"=>\"").collect();
            let key = elements.get(0).unwrap();
            let value = elements.get(1).unwrap();
            //println!("from {}=>{}", key, value);
            kv.insert(key.to_string(), value.to_string());
        }

        return kv;
    }

    pub fn to_hstore(kv : &HashMap<String, String>) -> String {
        let mut values: Vec<String> = Vec::new();
        for (key, value) in kv.iter() {
            let key_str  = key.to_string();
            let value_str = value.to_string();
            let key_value_str = format!("{}\"=>\"{}", key_str, value_str);
            values.push(key_value_str);
        }
        format!("\"{}\"", values.join("\", \""))
    }

}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::source::hstore::Hstore::{from_hstore, to_hstore};

    fn get_merge_attributes_str() -> &'static str {
        r#""1"=>"2", "id"=>"1234", "a"=>"2", "\"email\""=>"\"joe.blogs@gmail.com\"", "mobile"=>"61 466 333 222""#
    }

    fn get_key_values() -> HashMap<String, String> {
        let mut key_values = HashMap::new();
        key_values.insert("1".to_string(), "2".to_string());
        key_values.insert("\\\"email\\\"".to_string(), "\\\"joe.blogs@gmail.com\\\"".to_string());
        key_values.insert("mobile".to_string(), "61 466 333 222".to_string());
        key_values.insert("id".to_string(), "1234".to_string());
        key_values.insert("a".to_string(),"2".to_string());
        key_values
    }

    #[test]
    fn test_from_hstore() {
        let original = get_merge_attributes_str();
        let key_values = from_hstore(original.to_string());

        println!("key values {:?}", key_values);

        let email = key_values.get("\\\"email\\\"").unwrap().to_string();
        let mobile = key_values.get("mobile").unwrap().to_string();
        let id = key_values.get("id").unwrap().to_string();
        let one = key_values.get("1").unwrap().to_string();

        assert_eq!(email, "\\\"joe.blogs@gmail.com\\\"".to_string());
        assert_eq!(mobile, "61 466 333 222".to_string());
        assert_eq!(id, "1234".to_string());
        assert_eq!(one, "2".to_string());
    }

    #[test]
    fn test_to_hstore() {
        let expected_key_values = get_key_values();

        println!("expected key values {:?}", expected_key_values);

        let hstore_str = to_hstore(&expected_key_values);

        println!("hstore {}", hstore_str);

        let key_values = from_hstore(hstore_str);

        println!("key values {:?}", key_values);

        let email = key_values.get("\\\"email\\\"").unwrap().to_string();
        let mobile = key_values.get("mobile").unwrap().to_string();
        let id = key_values.get("id").unwrap().to_string();
        let one = key_values.get("1").unwrap().to_string();

        assert_eq!(email, "\\\"joe.blogs@gmail.com\\\"".to_string());
        assert_eq!(mobile, "61 466 333 222".to_string());
        assert_eq!(id, "1234".to_string());
        assert_eq!(one, "2".to_string());
    }

}