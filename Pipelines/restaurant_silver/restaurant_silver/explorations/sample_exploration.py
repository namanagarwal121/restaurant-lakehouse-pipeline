# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.bronze.customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.bronze.restaurants;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.bronze.menu_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.bronze.customer_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.fact_order_items

# COMMAND ----------

