# Databricks notebook source
# MAGIC %sql
# MAGIC with 
# MAGIC table_a as (
# MAGIC   select 1 as column_a
# MAGIC   union all select 1
# MAGIC   union all select 0
# MAGIC   union all select NULL
# MAGIC   union all select NULL
# MAGIC ),
# MAGIC table_b as (
# MAGIC   select 1 as column_b
# MAGIC   union all select 0
# MAGIC   union all select 0
# MAGIC ),
# MAGIC inner_join as (
# MAGIC   select count(*) as count
# MAGIC   from table_a 
# MAGIC   inner join table_b 
# MAGIC     on column_a = column_b
# MAGIC ),
# MAGIC left_join as (
# MAGIC   select count(*) as count
# MAGIC   from table_a 
# MAGIC   left join table_b 
# MAGIC     on column_a = column_b
# MAGIC ),
# MAGIC right_join as (
# MAGIC   select count(*) as count
# MAGIC   from table_a 
# MAGIC   right join table_b 
# MAGIC     on column_a = column_b
# MAGIC ),
# MAGIC full_outer_join as (
# MAGIC   select count(*) as count
# MAGIC   from table_a 
# MAGIC   FULL OUTER JOIN table_b 
# MAGIC     on column_a = column_b
# MAGIC )
# MAGIC
# MAGIC select 'inner' as join_type, * from inner_join
# MAGIC union all select 'left' as join_type, * from left_join
# MAGIC union all select 'right' as join_type, * from right_join
# MAGIC union all select 'full outer' as join_type, * from full_outer_join
