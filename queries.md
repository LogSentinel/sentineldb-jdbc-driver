# Description of various types of queries

- For `INSERT` queries, we extract values inserted. If the respective column is marked as indexed, the value is transformed (in addition to being encrypted) and the transformed value is stored in a lookup column.
- For `UPDATE` queries we extract the updated values as well as the where clause. In case the updated values are indexed, we fetch the previous values of the affected records, extract the IDs from there. The where clause is transformed to use the transformed value and the lookup column.
- For `SELECT` queries we transform the where clause to use the transformed value and the lookup column. 

Note: if a column is marked as "analyzed", then keywords need to be extracted. In those cases, instead of a lookup column, use a lookup table.