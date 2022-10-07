pub mod Json {

    use std::collections::HashMap;
    use lazy_static::lazy_static;
    use crate::source::clean_quotes;
    use regex::Regex;

    pub fn clean_preceding_braces(s: String) -> String {
        lazy_static! {
            static ref PRECEDING_BRACES_RE: Regex = Regex::new(r#"^\{"#).unwrap();
        }
        PRECEDING_BRACES_RE.replace_all(&s, "").to_string()
    }

    pub fn clean_trailing_braces(s: String) -> String {
        lazy_static! {
            static ref TRAILING_BRACES_RE: Regex = Regex::new(r#"\}$"#).unwrap();
        }
        TRAILING_BRACES_RE.replace_all(&s, "").to_string()
    }

    pub fn clean_braces(s: String) -> String {
        clean_trailing_braces(clean_preceding_braces(s))
    }

    pub fn from_json(s: String) -> HashMap<String, String> {
        let mut kv = HashMap::new();
        let clean_string = clean_quotes(clean_braces(s));
        for values in clean_string.split("\", \"") {
            let elements: Vec<&str> = values.split("\": \"").collect();
            let key = elements.get(0).unwrap();
            let value = elements.get(1).unwrap();
            //println!("from {}: {}", key, value);
            kv.insert(key.to_string(), value.to_string());
        }

        return kv;
    }

    pub fn to_json(kv : &HashMap<String, String>) -> String {
        let mut values: Vec<String> = Vec::new();
        for (key, value) in kv.iter() {
            let key_str  = key.to_string();
            let value_str = value.to_string();
            let key_value_str = format!("{}\": \"{}", key_str, value_str);
            values.push(key_value_str);
        }
        format!("{{\"{}\"}}", values.join("\", \""))
    }

}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::source::json::Json::{from_json, to_json};

    fn get_merge_attributes_str() -> &'static str {
         r#"{"1": "1234", "3": "4", "a": "2", "email": "joe.blogs@gmail.com"}"#
    }

    fn get_key_values() -> HashMap<String, String> {
        let mut key_values = HashMap::new();
        key_values.insert("1".to_string(), "1234".to_string());
        key_values.insert("email".to_string(), "joe.blogs@gmail.com".to_string());
        key_values.insert("3".to_string(), "4".to_string());
        key_values.insert("a".to_string(),"2".to_string());
        key_values
    }

    #[test]
    fn test_from_json() {
        let original = get_merge_attributes_str();
        let key_values = from_json(original.to_string());

        println!("key values {:?}", key_values);

        let email = key_values.get("email").unwrap().to_string();
        let a = key_values.get("a").unwrap().to_string();
        let one = key_values.get("1").unwrap().to_string();

        assert_eq!(email, "joe.blogs@gmail.com".to_string());
        assert_eq!(a, "2".to_string());
        assert_eq!(one, "1234".to_string());
    }

    #[test]
    fn test_to_json() {
        let expected_key_values = get_key_values();

        println!("expected key values {:?}", expected_key_values);

        let json_str = to_json(&expected_key_values);

        println!("json {}", json_str);

        let key_values = from_json(json_str);

        println!("key values {:?}", key_values);

        let email = key_values.get("email").unwrap().to_string();
        let a = key_values.get("a").unwrap().to_string();
        let one = key_values.get("1").unwrap().to_string();

        assert_eq!(email, "joe.blogs@gmail.com".to_string());
        assert_eq!(a, "2".to_string());
        assert_eq!(one, "1234".to_string());
    }

}