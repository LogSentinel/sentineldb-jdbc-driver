# Description of various types of queries and fields

- For `INSERT` queries, we extract values inserted. If the respective column is marked as indexed, the value is transformed (in addition to being encrypted) and the transformed value is stored in a lookup column.
- For `UPDATE` queries we extract the updated values as well as the where clause. In case the updated values are indexed, we simply generate a new recordId for the new values. The where clause is transformed to use the transformed value and the lookup column.
- For `SELECT` queries we transform the where clause to use the transformed value and the lookup column. 

Note: if a column is marked as "analyzed", then keywords need to be extracted. In those cases, instead of a lookup column, use a lookup table that maps the the lookup key to the ID of the record. Note that this is a rarer scenario, as full-text search within sensitive fields is not expected to be common.
Note: we create a separate UUID for every insert/update instead of using the original ID column to use for record-level encryption, as during insert we can't always know auto-generated field values. We can fetch it after insertion and encrypt it then, but that leaves unencrypted data in the database for a brief period of time, which can be intercepted in multiple ways.

There are four states of a field:
- regular (not defined in the SentinelDB schema) - we do nothing with it
- sensitive (defined in the SentinelDB schema) - we encrypt it
- indexed sensitive (defined as "indexed" in the SentinelDB schema) - we encrypt it and we make it searchable by exact match
- analyzed sensitive (defined as "analyzed" in the SentinelDB schema) - we encrypt it and we make it searchable by keyword match (splitting the text into multiple keywords)

The allowed data types for the different fields are:
- regular - any data type
- sensitive - varchar, text, clob, blob
- indexed sensitive - varchar
- analyzed sensitive - varchar, text, clob 