# PG-ANONYMIZER
### An anonymization tool for Postgres database dumps.
This tool takes a Postgres dump of a database and replaces desired column values with new, randomized values and outputs an anonymized dump.

## How to use
Create a file called config.json next to the executable. This file will be read by the tool to decide how the dump will be processed. Example config:

```json
{
  "public.\"Users\"": {
    "ignore_rows": {
      "Email": [
        "admin@domain.com"
      ]
    },
    "columns": {
      "Email": {
        "persist": true,
        "type": "email"
      }
    }
  }
}
```

Using this config the table public."Users" will have the column "Email"s values anonymized, unless the "Email" column contains any of the values specified in "ignore_rows" on the table config level. In that case the whole row will be left untouched. Setting "persist" to true will cause all instances of the original value across the dump to be replaced with the same newly generated value.

Having created this file you can run the tool as such:
``./pg-anonymizer dump.sql > anonymized-dump.sql``

## Options
| Name    | Example                     | Description                                          |
|---------|-----------------------------|------------------------------------------------------|
| config  | -config=path/to/config.json | Path to config file to use. Defaults to "config.json" |


## Config reference

### Table level configuration
| Name       | Values                 | Description                                                                                                                                           |
|------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
|ignore_rows | map[string][]string    | A map of string arrays containing values that will cause the row to be left untouched. Keys should be the name of the column to look for the value in |
|columns | map[string]coumnConfig | A map of column anonymization configuration. Keys should be the name of the column to apply this to.                                                  |

### Column level configuration
| Name        | Values                | Description                                                                                                          |
|-------------|-----------------------|----------------------------------------------------------------------------------------------------------------------|
| type | text, email, json, text_array | The type of column the configuration applies to.                                                                     
| persist | true, false           | Indicates wether the newly generated value must be persisted between tables.                                         |
| set_null | true, false           | If true, column value will be set to null regardless of value.                                                       |
| max_length | integer               | The maximum length of the newly generated value. If omitted length of replaced value will be used.                   |
| suffixes | ["-SUFF1", "-1FFUS" ] | A list of string suffixes that will, if encountered on the replaced value, be attached to the newly generated value. |
| keys | ["key", "other_key" ] | If type is "json" the keys in this list are the keys in the json object that will have their values replaced.        |

## Issues
Due to value persisting big databases will use A LOT of memory when being processed. 

## To do
- Take dump from stdin
- Reduce memory consumption
- Output to specified file
- More data and column types