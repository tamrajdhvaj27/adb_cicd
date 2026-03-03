-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE st_orders
AS
select *
from STREAM(samples.tpch.orders)

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW agg_orders
AS
select 
count(o_orderkey) as cnt_orders,
o_orderstatus
from LIVE.st_orders
GROUP by o_orderstatus
order by o_orderstatus

