* https://www.elastic.co/docs/reference/query-languages/esql/commands/where


```sql
ROW a = 1, b = 4, c = 3
| WHERE c-a IN (3, b / 2, a)
```


```sql
ROW a = "a", b = "b"
| WHERE "a" IN (a, b)
```
ok

```sql
ROW a = "a | a", b = "b | b"
| WHERE "a | a" IN (a, b)
```
ok


```sql
FROM op_testgroups
| WHERE id_run IN ("ch_hans_1 :: 2026-02-26_20-54-50-CET", "ch_hans_1 :: 2026-02-26_08-36-48-CET")
| KEEP id_group
| SORT id_group DESC
| LIMIT 1000
```


https://www.elastic.co/docs/explore-analyze/query-filter/languages/esql-kibana#esql-multi-values-controls
