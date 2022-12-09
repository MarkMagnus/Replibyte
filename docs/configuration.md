## Example

Here is a configuration file including some transformations and different options like the database subset.

```yaml
encryption_key: $MY_PRIVATE_ENC_KEY # optional - encrypt data on datastore
source:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
  database_subset: # optional - downscale database while keeping it consistent
    database: public
    table: orders
    strategy_name: random
    strategy_options:
      percent: 50
    passthrough_tables:
      - us_states
  transformers: # optional - hide sensitive data
    - database: public
      table: employees
      columns:
        - name: last_name
          transformer_name: random
        - name: birth_date
          transformer_name: random-date
        - name: first_name
          transformer_name: first-name
        - name: email
          transformer_name: email
        - name: username
          transformer_name: keep-first-char
    - database: public
      table: customers
      columns:
        - name: phone
          transformer_name: phone-number
  only_tables: # optional - dumps only specified tables.
    - database: public
      table: orders
    - database: public
      table: customers
datastore:
  aws:
    bucket: $BUCKET_NAME
    region: $S3_REGION
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
```
