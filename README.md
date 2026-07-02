<img src="https://cdn.prod.website-files.com/677c400686e724409a5a7409/6790ad949cf622dc8dcd9fe4_nextwork-logo-leather.svg" alt="NextWork" width="300" />

# Build a Delta Lake ETL Pipeline

**Project Link:** [View Project](https://nextwork.ai/projects/bf6fb326-95f0-4154-95e5-246aba8f2101)

**Author:** balraj kular  
**Email:** balrajkular35@gmail.com

---

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_38w120a4)

## Project Overview: Building a Production-Style Delta Lake ETL Pipeline

### Goals and motivation

In this project, I'm building a complete, production-style ETL pipeline in Databricks Free Edition that processes financial transaction data. I'll be ingesting raw data into a bronze Delta Lake table exactly as it arrives, then transforming and cleaning it into a silver table using MERGE/upsert operations for deduplication. Along the way, I'll implement schema evolution to handle new fields automatically, add data quality checks to catch issues early, and schedule everything as a multi-task Databricks job that runs daily without manual intervention. I'll also complete a secret mission by building a quality audit table that tracks data health over time, helping me spot gradual quality declines. By the end, I'll have hands-on experience with the same patterns and tools used by data engineering teams in production.

## Setting Up Databricks Free Edition with Unity Catalog

### Workspace and catalog setup

In this step, we're laying the groundwork for our ETL pipeline by setting up a governed storage layer using Unity Catalog. First, we'll explore Catalog Explorer to see how Unity Catalog organizes all data assets across your workspace using a three-level namespace: catalog.schema.table (or volume). This hierarchy makes every dataset fully addressable, discoverable, and consistently governed with fine-grained access controls and metadata tracking.

Next, we'll create a managed volume named raw-data inside a chosen catalog and schema. A volume is a logical container for storing non-tabular files—like JSON, CSV, or Parquet—directly within Unity Catalog. This volume will act as our raw landing zone, where we'll ingest source transaction files before any transformation or cleaning begins. By using a managed volume, we ensure that the data is automatically tracked, versioned, and secured under Unity Catalog's governance model, making it easy to audit, tag, and control access.

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_3mmj7hsq)

## Ingesting Raw Transaction Data into a Bronze Delta Lake Table

### Ingestion strategy

In this step, we're building the ingestion layer of our ETL pipeline by creating a Python notebook that defines raw transaction data and writes it into a bronze Delta Lake table. The bronze layer serves as our raw landing zone, storing data exactly as it arrived—preserving every field and quirk without any transformations.

We'll accomplish this by:

Creating a Python notebook for our ingestion logic.
Defining raw transaction data inline and loading it into a PySpark DataFrame.
Writing that DataFrame to a bronze Delta Lake table.
Verifying the write was successful using SQL queries.
This establishes the foundation of our Delta Lake architecture, giving us an immutable, versioned source of truth before any downstream processing begins.

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_pvt2acpe)

### Bronze layer design decisions

The bronze table contains six fields from the raw transaction data: transaction_id, customer_id, amount, currency, transaction_date, and merchant. These fields reflect exactly what was defined in the inline raw_data list—no transformations, renaming, or filtering applied. The table includes all rows as-is, even those with intentional data quality issues like None values for transaction_id and customer_id, a negative amount (-20.00), and various currency types (USD, EUR, GBP). Every field and every value is preserved exactly as it was defined in the source simulation. 
We keep it unmodified to preserve an immutable source of truth for audit trails, debugging, and reprocessing. It allows us to handle schema evolution gracefully, supports future analytical flexibility, and enables data quality monitoring by comparing raw vs. cleaned records. The bronze layer stays untouched so downstream transformations can always reference the original payload with confidence.

## Transforming and Upserting Data into a Silver Table with MERGE

### Transformation and upsert approach

In this step, we're building the silver layer of our ETL pipeline—the cleaning and validation stage. Raw data from the bronze table is messy, with issues like inconsistent data types, missing values, and invalid records. Our goal is to transform this raw data into a trusted, query-ready dataset that downstream consumers can rely on.

We'll read the bronze data and apply PySpark transformations to cast columns to proper data types, filter out bad records (like null transaction IDs or negative amounts), and enrich the data with a processing timestamp. Then we'll use the DeltaTable MERGE API to upsert cleaned records into a silver Delta table—handling both inserts and updates in one operation to avoid duplicates. Finally, we'll verify the results by querying the silver table to confirm it contains only valid, deduplicated records with correct data types.

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_4jbwl7sc)

### How MERGE handles matched and unmatched records

When MERGE finds a matching transaction_id already in the silver table, it updates the existing record with the new values—keeping the table current without creating duplicates. This handles corrections or late-arriving data from the source.

When MERGE does not find a match, it inserts the new row as a fresh record for first-time transactions.

Together, this makes MERGE an upsert (update + insert). It ensures the silver table maintains one authoritative record per transaction, no matter how many times the pipeline runs. This approach is idempotent—re-running the pipeline on the same data produces the same result without duplicates. Unlike a full table overwrite, MERGE only touches changed rows, saving compute time and preserving the table's history.

## Implementing Schema Evolution to Handle New Data Fields

### Schema evolution strategy

n this step, we're preparing our pipeline to handle schema evolution—the reality that data sources change over time in production. New fields get added, columns get renamed, and our pipeline needs to adapt without breaking or requiring manual intervention. We'll simulate this scenario by adding a new category field to incoming transaction data and teach both our bronze and silver layers to evolve gracefully.

First, we'll write a second batch of data to the bronze table that includes the new category column, using Delta Lake's automatic schema evolution to merge the new schema with the existing table structure. Then, we'll update our silver transformation to handle this new column, using the MERGE WITH SCHEMA EVOLUTION feature to automatically add the category field to the silver table without disrupting existing records. Finally, we'll verify that old records (without the field) and new records (with the field) coexist seamlessly in the evolved silver table.

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_p41nsb39)

### Handling existing records after schema change

Schema evolution handles existing records gracefully by assigning them a null value for the new column. When the category field is added to the silver table via withSchemaEvolution(), Delta Lake does not backfill or modify old rows—it simply adds the column to the table schema, and all pre-existing records automatically receive null for that column. This preserves the integrity of historical data while allowing new records to populate the field with actual values.

This behavior is exactly what we want in production. Old transactions that never had a category remain untouched and queryable, while new transactions include the category information. Downstream consumers can handle the nulls appropriately—either by filtering, providing defaults, or waiting for the data to be enriched later. The pipeline stays running without manual intervention, and no data is lost or corrupted during the schema change.

## Adding Data Quality Checks and Scheduling the Pipeline as a Databricks Job

### Quality checks and job orchestration

In this step, we're adding data quality guardrails and production automation to our ETL pipeline. Data quality can degrade silently over time, so we'll add automated assertions to our silver transformation that check for issues like null transaction IDs or negative amounts. If any assertion fails, the pipeline stops immediately—preventing bad data from reaching downstream consumers.

Then we'll wire everything together by creating a Databricks job—a scheduled, multi-task workflow that runs our bronze ingestion and silver transformation notebooks in the correct order with task dependencies. This replaces manual execution with a fully automated process that runs daily. Finally, we'll schedule the job and verify it runs successfully, giving us an end-to-end pipeline that ingests, cleans, and validates data automatically with zero human intervention.

### What the three quality checks validate

The three checks validate critical data integrity rules that downstream consumers depend on:

Check 1: No null transaction_ids – Ensures every record has a valid primary key. A null transaction_id would break joins, reporting, and downstream logic, so we reject these rows immediately.

Check 2: All amounts within valid range (0 to 1,000,000) – Validates that amounts are reasonable and non-negative. This provides a second line of defense against outliers or data corruption that could skew analytics.

Check 3: No duplicate transaction_ids – Confirms the MERGE upsert logic is working and no duplicates exist. This prevents double-counting in downstream aggregations.

If any check fails, the notebook raises an error and stops execution, preventing bad data from reaching consumers.

## Secret Mission: Building a Data Quality Audit Table for Ongoing Monitoring

![Image](https://nextwork.ai/glowing_green_jolly_blackberry/uploads/bf6fb326-95f0-4154-95e5-246aba8f2101_w6iop0pa)

### Why an audit table catches issues that assertions miss

Simple assertions stop the pipeline when something fails, but they leave no trace when everything passes. An audit table records every check run—pass or fail—building a historical dataset over time. This reveals gradual trends that assertions alone would miss. For example, completeness for the category field might drop from 95% to 60% over a month. Assertions only catch it when it crosses a hard threshold, but by then the damage is done and you're left without context. An audit table lets you query historical results to spot declines early, identify which checks fail most often, compare pass rates across runs, and correlate quality issues with upstream changes. It turns quality from a binary "pass/fail" checkpoint into an ongoing, measurable metric that the team can monitor, investigate, and improve proactively—rather than reacting to sudden pipeline breaks.

## Reflections and Key Takeaways

### Tools and concepts mastered

I learned how to build a production-ready ETL pipeline in Databricks using Delta Lake and PySpark. Key tools included Databricks notebooks, Unity Catalog for three-level data organization, and Delta Lake for ACID transactions and schema evolution.

On the concepts side, I built bronze and silver tables following the medallion architecture—storing raw data unchanged in bronze, then cleaning and deduplicating it in silver using MERGE/upsert operations. I implemented schema evolution to handle new columns automatically, added data quality assertions to halt the pipeline on failures, and created a Databricks Job to schedule everything daily. Finally, I built a quality audit table that tracks check results over time, helping monitor gradual quality declines that simple assertions would miss.

### Time and challenges

It took me approximately 45 to 55 minutes to complete this project from start to finish. This includes setting up the Databricks environment, creating the three notebooks, writing and running the Python and SQL code for each step, implementing schema evolution, adding data quality checks, configuring the Databricks job with task dependencies, and completing the secret mission by building the quality audit table.

The first run of each notebook took a bit longer because serverless compute needed to spin up, but subsequent cells and manual job runs executed much faster. Overall, the project was well-paced and the step-by-step guidance made it easy to follow without getting stuck for too long.

### Looking ahead

I did this project today to learn how to build a complete ETL pipeline in Databricks using Delta Lake and PySpark. I wanted hands-on experience with the medallion architecture—creating bronze and silver tables, using MERGE for upserts, handling schema evolution, and scheduling everything as a Databricks job with data quality checks. The secret mission on building a quality audit table was especially valuable for learning how to monitor data health over time.

Another skill I want to learn is working with Azure Data Factory to orchestrate data pipelines across cloud and on-premises sources. I'm interested in understanding how to use copy activities, data flows, and triggers to build hybrid ETL solutions that integrate with Databricks and Azure Synapse.

---

*Built with [NextWork](https://nextwork.ai) - [View this project](https://nextwork.ai/projects/bf6fb326-95f0-4154-95e5-246aba8f2101)*
