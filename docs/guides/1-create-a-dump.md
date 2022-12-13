# Create a dump

## Configuration

To use Replibyte, you need to use to a dump from your production or follower database. 
Only one option use Replibyte to make a dump or better yet a partial.

Don't dump manually use pgdump they won't be compatible with this version of replibyte
Don't use files dump by native database tools. Strategies are now in place that make this impractical.

### make a dump with Replibyte

To let Replibyte creating a dump from your database for you, you need to update your `conf.yaml` file with the source 
connection URI from your production database as a property.

Pick the example that fit the database you are using.

<details>

<summary>PostgreSQL</summary>

```yaml
source:
  connection_uri: postgres://[user]:[password]@[host]:[port]/[database]
```

</details>

<details>

By using [Transformers](/docs/transformers), you can change on the fly your database data. 
Let's say we have the following structure for a table `employees`

```sql
CREATE TABLE public.customers (
    id integer NOT NULL,
    merchant_id integer not null,
    first_name character varying(30) NOT NULL,
    last_name character varying(30) NOT NULL,
    email character varying(2048) NOT NULL,
    mobile character varying(24),
    attributes jsonb default '{}',
    cache hstore default '',
    access_key character varying(30)
);
```

with the following entries:

```sql

INSERT INTO public.customers (id, merchant_id, first_name, last_name, email, mobile, attributes, cache, access_key)
VALUES (1, 1, 'Mark', 'Magoo', 'mark.magoo@gmail.com', '1234274321', '{"alt_email": "mark.magoo2@gmail.com", "options": "1"}', 'confirmation_key=>129334', '34398409-980eu9');

INSERT INTO public.customers (id, merchant_id, first_name, last_name, email, mobile, attributes, cache, access_key)
VALUES (1, 2, 'Maria', 'Anders', 'maria.anders@gmail.com', '030-0074321', '{"alt_email": "anders.family@gmail.com", "options": "2"}', 'confirmation_key=>118374', '12934980-0.09.p');

INSERT INTO public.customers (id, merchant_id, first_name, last_name, email, mobile, attributes, cache, access_key)
VALUES (1, 2, 'Ana', 'Trujillo', 'ana@factchecker.com', '(5) 555-4729', '{"alt_mobile": "128937982392", "alt_email": "facts.info@gmail.com", "options": "1"}', 'confirmation_key=>129343', '932809u0-90809e9');

INSERT INTO public.customers (id, merchant_id, first_name, last_name, email, mobile, attributes, cache, access_key)
VALUES (1, 3, 'Antonio', 'Moreno', 'anto.moreno@gmail.com', NULL, '{"alt_email": "anto.moreno@hotmail.com", "options": "1"}', 'confirmation_key=>123948', '90809oeuhn.-rgroeut');


select *, attributes->>'alt_email' as alternative_email,
    cache->'confirmation_key' as last_email_confirmation_key
from customers;


```
and you want to mask/transform `last_name`, `email` and `mobile` fields.
and hide specific attributes in `attributes` and `cache` fields.
and completely obliterate `access_key field.`
and you don't want all records that belong to customer from merchant_id == 2
You can use the following configuration in your `conf.yaml` file.

```yaml title="source and transformers in your conf.yaml"
source:
  connection_uri: postgres://user:password@host:port/db
  transformers:
    - database: public
      table: customers
      columns:
        - name: last_name
          transformer_name: random
        - name: mobile
          transformer_name: mobile-number
          transformer_options:
            country_code: 1
            length: 10 
        - name: contact_email
          transformer_name: email
        - name: attributes
          transformer_name: jsonb-attr
          transformer_options:
            - attribute: alt_email 
              transformer_name: email 
            - attribute: alt_mobile 
              transformer_name: mobile-number
              transformer_options:
                country_code: 1
                length: 11
        - name: cache
          transformer_name: hstore-attr
          transformer_options:
            - attribute: confirmation_key 
              transformer_name: random
        - name: access_key
          transformer_name: blank           
```

## Run

```yaml title="Add your datastore in your conf.yaml"
datastore:
  aws:
    bucket: my-replibyte-dumps
    region: us-east-2
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
      session_token: XXX # optional
```


<details>

<summary>Make a dump with Replibyte</summary>

```shell
replibyte -c conf.yaml dump create
```

</details>
</details>