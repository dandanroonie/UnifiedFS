/*  

Like Jupyter is a streamlined end - to - end demo for JPMC 
Zero To Snowflake JPMC Day 1: 
1. Overview 
2. Cost Financial Governance


Zero to SF Day 2: 
3. Optimized Data Transformation
    Loading Data Manually by file
    Streaming Data using Streams/ tasks Continuous

4. Optimize Performance
    Showing a Query plan and result 
    Caching - Run a cold query
    Caching - Run a hot query
    Activity-> History

5. Data Engineering
    Generate test data
    User defined functions


--------------------------------------------------*/

CREATE DATABASE IF NOT EXISTS JPMC; 
USE DATABASE JPMC;
CREATE SCHEMA IF NOT EXISTS DEMO; 

USE ROLE ACCOUNTADMIN;

-- https://docs.snowflake.com/en/sql-reference/sql/create-warehouse.html
-- Teraform configured by Jade
-- *******************************************
-- ** 2. Cost Financial Governance
-- **   2.1 Virtual Warehouses & Settings
--      2.2 Resource monitors
-- Fraction of 1 second (12 times)
CREATE OR REPLACE WAREHOUSE COSTCENTER_ESG_TEST WITH 
    WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 INITIALLY_SUSPENDED = TRUE RESOURCE_MONITOR = JPMC_JADE_MONITOR AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD' ;


ALTER WAREHOUSE COSTCENTER_ESG_TEST RESUME IF SUSPENDED;

-- ******************************
-- ** 2.3 Manual WH Suspend

ALTER WAREHOUSE COSTCENTER_ESG_TEST SUSPEND; 

-- ** c. Warehouse size is small  
ALTER WAREHOUSE COSTCENTER_ESG_TEST SET WAREHOUSE_SIZE = 'SMALL'; 

-- So here's our context: 
select current_warehouse(), current_database(), current_schema();

-- *******************************
-- ** 2.4 Timeout controls

-- to begin, let's look at a few parameters available on our Warehouse
SHOW PARAMETERS IN WAREHOUSE COSTCENTER_ESG_TEST;

-- above we saw three available parameters, let's start by adjusting the two related to timeouts

    /**
     STATEMENT_TIMEOUT_IN_SECONDS: Timeout in seconds for statements: statements are automatically canceled if they
      run for longer; if set to zero, max value (604800) is  enforced.
     STATEMENT_QUEUED_TIMEOUT_IN_SECONDS: Timeout in seconds for queued statements: statements will automatically be
      canceled if they are queued on a warehouse for longer than this  amount of time; disabled if set to zero.
    **/


-- adjust STATEMENT_TIMEOUT_IN_SECONDS on WAREHOUSE to 30 minutes
-- 
ALTER WAREHOUSE COSTCENTER_ESG_TEST 
SET statement_timeout_in_seconds = 1800; -- 1800 seconds = 30 minutes 


-- *****************************
-- ** 3. Optimized Data Transformation
-- 3a. Loading Data Manually by file
-- what do these files look like within the blob storage?
-- ** Stage Pointer to the S3 Buckets ... 

LIST @frostbyte_esg.public.portfolio_data_stage/json/;
select count(distinct METADATA$FILENAME) from @frostbyte_esg.public.portfolio_data_stage/json/;


-- what do the contents of these Daily Portfolio holdings files look like?
SELECT TOP 5 $1
FROM @frostbyte_esg.public.portfolio_data_stage/json
    (FILE_FORMAT=>frostbyte_esg.public.json_ff);

        /*---
         feature note: Snowflake knows how to automatically convert data from JSON, Avro,
          ORC, or Parquet format to an internal hierarchy of ARRAY, OBJECT, and VARIANT
          data and store that hierarchical data directly into a VARIANT.
        ---*/  

-- create a daily_portfolio_positions table with a VARIANT column to load our JSON into
CREATE OR REPLACE TABLE daily_portfolio_positions (obj variant);
-- CREATE OR REPLACE TABLE frostbyte_esg.raw_.daily_portfolio_positions (obj variant);

-- scale up our warehouse to medium just for ingest 
ALTER WAREHOUSE COSTCENTER_ESG_TEST SET warehouse_size = 'Medium';

-- *****************************
-- 3a. Loading Data Manually by file

-- ingest all of our portfolio holdings files into our newly created daily portfolio positions table
-- https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
COPY INTO 
    daily_portfolio_positions
FROM @frostbyte_esg.public.portfolio_data_stage/json
    FILE_FORMAT = frostbyte_esg.public.json_ff;

-- 200K+ positions read in
SELECT COUNT (*) FROM daily_portfolio_positions;

-- scale our warehouse back down to extra small since ingest is complete 
ALTER WAREHOUSE COSTCENTER_ESG_TEST SUSPEND; 
ALTER WAREHOUSE COSTCENTER_ESG_TEST SET warehouse_size = 'XSmall';
show warehouses like 'cost%';
--
--************************
-- 3.2. Streaming Data using Streams/ tasks - continuous ingestion 
-- data can be brought in using external tables and tasks stream and pipes - as it was on this sheet- https://app.snowflake.com/us-east-1/coa26304/w40NO20SLzVV#query
--create stage for trips
-- DON'T RUN - not doing Trips Streaming 
    create or replace stage pipe_data_trips url = 's3://snowflake-workshop-lab/snowpipe/trips/' file_format=(type=csv);
    list @pipe_data_trips;
    -- create the trips pipe using SNS topic
    
    create or replace pipe trips_pipe auto_ingest=true 
        aws_sns_topic='arn:aws:sns:us-east-1:484577546576:snowpipe_sns_lab' 
        as copy into trips_stream from @pipe_data_trips/;
        show pipes;
    --check trips pipe status
    select system$pipe_status('trips_pipe');



/*----------------------------------------------------------------------------------
Step 2 - 
 With our JSON files loaded, let's now take a look at the data in our table and
 leverage Snowflake's dot notation to flatten to object data into legible columns
 and create a view for downstream usage
----------------------------------------------------------------------------------*/
-- **********************************************************
-- ** 4. Optimize Performance

-- what does the data look like in our daily_portfolio_positions table?

SELECT 
    TOP 50 * 
FROM daily_portfolio_positions;

        /*---
         feature note: Snowflake supports SQL queries that access semi-structured data using
          special operators and functions. Individual elements in a VARIANT column can be 
          accessed using dot or bracket notation
        ---*/  

-- Larger query 3 tables one is SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.
select count(*) from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales;
select count(*) from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.item;


-- **********************************************************
--    4.1 Showing a Query plan and result 

-- result -cold - <6 sec  4x
---       -warm <2 sec.   4x  -- already scanned data is read. 
---       -hot < .15 sec   4x
--- *** TD: FS query 
--- *** Modify to show it going faster - inner order by. 


select  i_item_id,i_item_desc,i_category,i_class,i_current_price,sum(ws_ext_sales_price) as itemrevenue,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over (partition by i_class) as revenueratio
from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.web_sales,SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.item,SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.date_dim
where ws_item_sk = i_item_sk 
    and i_category in ('Women','Sports')
    --and i_current_price = 2.99
    and ws_sold_date_sk = d_date_sk
    and d_date between cast('1999-04-17' as date) 
    and dateadd(day,30,to_date('1999-04-17'))
group by i_item_id,i_item_desc,i_category,i_class,i_current_price
order by i_category,i_class,i_item_id,i_item_desc,revenueratio
limit 100;



--    4b. Caching (Run a cold and hot query)
--          Cold .4 sec
--          Hot .08 sec 

ALTER WAREHOUSE COSTCENTER_ESG_TEST SUSPEND;  -- Make cold
ALTER SESSION SET USE_CACHED_RESULT = false;  -- Warm 
ALTER SESSION SET USE_CACHED_RESULT = true;   -- Hot




-- ** 5. ELT and Data Engineering 
-- let's use dot notation to flatten our results into legible columns for our flagship portfolio
SELECT 
    obj:DATE::date AS date,
    obj:PORTFOLIO::string AS portfolio,
    obj:TICKER::string AS ticker,
    obj:TICKER_REGION::string AS ticker_region,
    obj:SECURITY_NAME::string AS security_name,
    obj:INTERNAL_ESG_SCORE::float AS internal_esg_score,
    obj:TOTAL_SHARES::number AS total_shares,
    obj:CHANGE_IN_SHARES::number AS change_in_shares
FROM daily_portfolio_positions dpp
WHERE 1=1
    AND obj:PORTFOLIO = 'SnowCap Flagship Portfolio'
ORDER BY obj:DATE DESC;

-- drop view frostbyte_esg.harmonized.daily_portfolio_positions_v;
-- using the query above, let's create a harmonized view over the data
CREATE OR REPLACE MATERIALIZED VIEW daily_portfolio_positions_v
    AS
SELECT 
    obj:DATE::date AS date,
    obj:PORTFOLIO::string AS portfolio,
    obj:TICKER::string AS ticker,
    obj:TICKER_REGION::string AS ticker_region,
    obj:SECURITY_NAME::string AS security_name,
    obj:INTERNAL_ESG_SCORE::float AS internal_esg_score,
    obj:TOTAL_SHARES::number AS total_shares,
    obj:CHANGE_IN_SHARES::number AS change_in_shares
FROM
daily_portfolio_positions dpp
WHERE 1=1;
--use this if regular view
--ORDER BY obj:DATE DESC;


-- leveraging our new view, what are our Tesla holdings over time against all portfolios?
SELECT 
    dpp.date,
    dpp.portfolio,
    dpp.ticker,
    dpp.internal_esg_score,
    dpp.total_shares,
    dpp.change_in_shares
FROM daily_portfolio_positions_v dpp
WHERE 1=1
    AND ticker = 'TSLA'
ORDER BY date DESC;

-- **5a. Generate some data (trade blotter) More on Trade blotter: https://app.snowflake.com/us-east-1/coa26304/w1MX9c9M150D#query
CREATE OR REPLACE TABLE Blotter (
   TDate DATE,
   TraderID INTEGER,
   Proceeds INTEGER,
   Side STRING
);

-- Fill in Some data that is either 3 days old or 90-93 days old. 
INSERT INTO Blotter
    (Tdate,TraderID, Proceeds, Side)
SELECT CURRENT_DATE() - (UNIFORM(0, 2, RANDOM() )+ 90 *(UNIFORM(0, 1, RANDOM() ))) AS Tdate,
    UNIFORM(1, 10, RANDOM() ) as TraderID,
    UNIFORM(1, 100, RANDOM() ) as Proceeds,
    get(array_construct('BUY', 'SELL', 'SHORT', 'COVER'), uniform(0, 3, random())) AS Side
FROM TABLE(GENERATOR(rowcount => 100) ) ;

-- Order the trade blotter by date as it will be in practice

CREATE OR REPLACE TABLE Blotter AS
    SELECT * FROM Blotter ORDER BY tdate ;
SELECT COUNT (*) FROM Archival.Archival.Blotter;

SELECT * FROM Blotter ;


/*----------------------------------------------------------------------------------
Step 3 - 
 Happy with our first party portfolio holdings view, let's now marry our first party
 data including our internal ESG score with FactSet data from the Snowflake Data 
 Marketplace and create an analytic view for downstream use.
 Data Marketplace: Data Marketplace -> Search: TRUVALUE OR FactSet
            https://app.snowflake.com/marketplace/listing/GZT0ZGCQ51PW/factset-truvalue-labs-sasb-scores-datafeed?search=truvalue
----------------------------------------------------------------------------------*/

-- using ticker, join our newly created portfolio view with FactSet ESG and Market Data 
SELECT 
    dpp.date,
    dpp.portfolio,
    dpp.ticker,
    tdd.materiality_adj_insight,
    tdd.materiality_esg_rank,
    tdd.all_categories_adj_insight,
    tdd.all_categories_esg_rank,
    tdd.market_cap,
    tdd.price
FROM daily_portfolio_positions_v dpp
JOIN frostbyte_esg.harmonized.factset_ticker_detail_daily tdd
    ON dpp.date = tdd.date
    AND dpp.ticker_region = tdd.ticker_region
ORDER BY dpp.date DESC, dpp.portfolio;

ALTER WAREHOUSE COSTCENTER_ESG_TEST SET WAREHOUSE_SIZE = 'MEDIUM' ; 


--*** Now Show the BI - First scale up and turn up the timeout 
--** 7. Business Intelligence 
--    7a. Visualization within a query 
--    7b. Snowsight analytics: https://app.snowflake.com/us-east-1/coa26304/#/benchmarking-with-esg-dO9VQQ040

-- https://help.tableau.com/current/pro/desktop/en-us/examples_snowflake.htm
ALTER WAREHOUSE COSTCENTER_ESG_TEST SET AUTO_SUSPEND = 1200;

ALTER WAREHOUSE COSTCENTER_ESG_TEST RESUME IF SUSPENDED; 

ALTER WAREHOUSE COSTCENTER_ESG_TEST SET AUTO_SUSPEND = 600;
-- let's use the query above to promote this married data to downstream users as an analytics view

-- We can clone it: https://docs.snowflake.com/en/sql-reference/sql/create-clone.html
CREATE OR REPLACE TABLE BLOTTER_COPY CLONE BLOTTER;

-- ** 8. Snowgrid and Cross cloud resilience 
--    8a. Undrop 
--    8b. Time travel

DROP TABLE IF EXISTS BLOTTER_COPY;
DROP TABLE IF EXISTS BLOTTER_COPY_RESTORE;
SHOW TABLES LIKE 'BLOTTER_CO%';
UNDROP TABLE BLOTTER_COPY;


SELECT COUNT (*) FROM BLOTTER;
SELECT COUNT (*) FROM BLOTTER_COPY;

INSERT INTO BLOTTER_COPY
    (Tdate,TraderID, Proceeds)
SELECT CURRENT_DATE() - (UNIFORM(0, 2, RANDOM() )+ 90 *(UNIFORM(0, 1, RANDOM() ))) AS Tdate,
    UNIFORM(1, 10, RANDOM() ) as TraderID,
    UNIFORM(1, 100, RANDOM() ) as Proceeds
FROM TABLE(GENERATOR(rowcount => 100) ) ;

set query_id = last_query_id();

-- ** 8. Snowgrid and Cross cloud resilience 
--    8a. Undrop 
--    8b. Time travel
--Clone a table as it existed immediately before the execution of the specified statement (i.e. query ID):
create or replace table BLOTTER_COPY_RESTORE clone BLOTTER_COPY before (statement => $query_id);
-- The restore will look like the original! 
SELECT COUNT (*) FROM BLOTTER_COPY_RESTORE;
SELECT COUNT (*) FROM BLOTTER_COPY;
SELECT COUNT (*) FROM BLOTTER;

/* Recap Superset
Workflow proof points: 
0. JPMC Architecture 
1. Show the interface
2. Elastic Scale- Prepping to load: 
    2a. Compute Create Warehouse
    2b.  Spin up and Suspend
    2c. Shut down Resize - Alter
3. Optimizing Data by Loading it
    3a. Loading Data Manually by file
    3b. Streaming Data using Streams/ tasks Continuous
    
4. Elasting Scale - Part II  
    4a. Showing a Query plan and result 
    4b. Caching (Run a cold and hot query)

5. ELT and Data Engineering 
    5a. Generate some data (trade blotter)
    5b. Talk thru user defined functions 
    
6. Data sharing and Snowflake marketplace 

7. Business Intelligence 
    7a. Visualization within a query
    7b. Snowsight analytics
    7c. Tableau or Qlik - Open the package 
    
8. Snowgrid and Cross cloud resilience 
    8a. Undrop 
    8b. Time travel

*/ 



-- USE ROLE sysadmin;
CREATE OR REPLACE VIEW daily_portfolio_detail_v
    AS
SELECT 
    dpp.*,
    tdd.entity_name_value,
    tdd.entity_profile_long,
    tdd.market_cap,
    tdd.price,
    tdd.price_50d_moving_average,
    tdd.price_200d_moving_average,
    tdd.volume,
    tdd.materiality_adj_insight,
    tdd.materiality_ind_pctl,
    tdd.materiality_esg_rank,
    tdd.all_categories_adj_insight,
    tdd.all_categories_ind_pctl,
    tdd.all_categories_esg_rank
FROM daily_portfolio_positions_v dpp
JOIN frostbyte_esg.harmonized.factset_ticker_detail_daily tdd
    ON dpp.date = tdd.date
    AND dpp.ticker_region = tdd.ticker_region
ORDER BY dpp.date DESC, dpp.portfolio;


/*----------------------------------------------------------------------------------
Step 4 - 
 With our enriched view in place, let's look at how our internally calculated ESG score
 compares to what FactSet provides
----------------------------------------------------------------------------------*/

-- using our new view, what are our portfolio holdings average daily internal and FactSet
-- calculated ESG scores?
--USE ROLE data_engineer_esg;

SELECT
    dpd.date,
    dpd.portfolio,
    ROUND(AVG(dpd.internal_esg_score),2) AS avg_internal_esg_score,
    ROUND(AVG(dpd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight,
    ROUND(AVG(dpd.materiality_adj_insight),2) AS avg_materiality_adj_insight,
    ROUND(AVG(dpd.materiality_ind_pctl),2) AS avg_materiality_ind_pctl,
    ROUND(AVG(dpd.all_categories_ind_pctl),2) AS avg_all_categories_ind_pctl
FROM daily_portfolio_detail_v dpd
WHERE 1=1
    AND dpd.date >= '2021-03-01'
GROUP BY dpd.date, dpd.portfolio
ORDER BY dpd.date DESC, dpd.portfolio;


-- for our flagship portfolio, how does our internal ESG score compare to
-- FactSets all categories adjusted insight score over time?
    -- Optional Snowsight Chart: Type: Line | DATE(month): x-axis | AVG_INTERNAL_ESG_SCORE(average): Line | AVG_ALL_CATEGORIES_ADJ_INSIGHT(average): Line
SELECT
    dpd.date,
    dpd.portfolio,
    ROUND(AVG(dpd.internal_esg_score),2) AS avg_internal_esg_score,
    ROUND(AVG(dpd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight,
    ROUND((AVG(dpd.internal_esg_score) - AVG(dpd.all_categories_adj_insight)),2) AS internal_to_all_categories_delta
FROM daily_portfolio_detail_v dpd
WHERE 1=1
    AND dpd.date >= '2021-03-01'
    AND dpd.portfolio = 'SnowCap Flagship Portfolio'
GROUP BY dpd.date, dpd.portfolio
ORDER BY dpd.date DESC, dpd.portfolio;


-- what are our monthly ESG averages for our portfolios?
SELECT
    TO_VARCHAR(dpd.date, 'yyyy-mm') AS year_month,
    dpd.portfolio,
    ROUND(AVG(dpd.internal_esg_score),2) AS avg_internal_esg_score,
    ROUND(AVG(dpd.materiality_adj_insight),2) AS avg_materiality_adj_insight,
    ROUND(AVG(dpd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight,
    ROUND(AVG(dpd.materiality_ind_pctl),2) AS avg_materiality_ind_pctl,
    ROUND(AVG(dpd.all_categories_ind_pctl),2) AS avg_all_categories_ind_pctl
FROM daily_portfolio_detail_v dpd
WHERE 1=1
    AND dpd.date >= '2021-03-01'
GROUP BY year_month, dpd.portfolio
ORDER BY year_month DESC, dpd.portfolio;


/*----------------------------------------------------------------------------------
Step 5 - 
 Since we do not calculate ESG scores for as many companies as FactSet does, let's 
 now leverage their scores to see how our portfolios compare to S&P Index benchmarks
----------------------------------------------------------------------------------*/

-- what are the average ESG scores for the available S&P Index benchmarks?
SELECT
    TO_VARCHAR(bd.date, 'yyyy-mm') AS year_month,
    bd.benchmark_name,    
    ROUND(AVG(bd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight,
    ROUND(AVG(bd.materiality_adj_insight),2) AS avg_materiality_adj_insight,
    ROUND(AVG(bd.materiality_ind_pctl),2) AS avg_materiality_ind_pctl,
    ROUND(AVG(bd.all_categories_ind_pctl),2) AS avg_all_categories_ind_pctl
FROM frostbyte_esg.analytics.benchmark_detail_v bd
WHERE 1=1
    AND bd.date >= '2021-03-01'
GROUP BY year_month, bd.benchmark_name
ORDER BY year_month DESC, bd.benchmark_name;


-- how does our flagship portfolio compare to the S&P 500 and 1500?
    -- Viz Steps: Chart -> Bar -> YEAR_MONTH(x-axis) | NAME(series) | AVG_ALL_CATEGORIES_ADJ_INSIGHT(none) Bar
        -- Orientation: 2nd Option | Grouping: 1st Option | Order bars by: YEAR | Order/Series Direction: Ascending
WITH _union AS
((
    SELECT
        TO_VARCHAR(bd.date, 'yyyy-mm') AS year_month,
        bd.benchmark_name AS name,
        ROUND(AVG(bd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight
    FROM frostbyte_esg.analytics.benchmark_detail_v bd
    GROUP BY year_month, bd.benchmark_name
    ORDER BY year_month DESC, bd.benchmark_name
)
    UNION
(
    SELECT
        TO_VARCHAR(dpd.date, 'yyyy-mm') AS year_month,
        dpd.portfolio AS name,
        ROUND(AVG(dpd.all_categories_adj_insight),2) AS avg_all_categories_adj_insight
    FROM daily_portfolio_detail_v dpd
    GROUP BY year_month, dpd.portfolio
    ORDER BY year_month DESC, dpd.portfolio
))
SELECT * 
FROM _union
WHERE 1=1
    AND name IN ('SnowCap Flagship Portfolio','S&P Total U.S. Stock Market','S&P 500')
    AND year_month > '2021-06-01'
ORDER BY year_month DESC, name;


/*----------------------------------------------------------------------------------
Step 6 - 
 To assist our Portfolio Managers, let's locate and dive into the top monthly ESG
 score decliners that we hold in our portfolios
----------------------------------------------------------------------------------*/

-- leveraging FactSets ESG scores, let's find the monthly ESG decliners 
-- in our portfolios to have reviewed by our Portfolio Managers
WITH _monthly_portfolio_delta AS
(
SELECT
    TO_VARCHAR(dpd.date, 'yyyy-mm') AS year_month,
    dpd.ticker,
    dpd.security_name,
    dpd.portfolio,
    ROUND((ZEROIFNULL(
        AVG(dpd.materiality_adj_insight) - LAG(AVG(dpd.materiality_adj_insight)) OVER (
            PARTITION BY dpd.ticker, dpd.portfolio ORDER BY TO_VARCHAR(dpd.date, 'yyyy-mm')
        ))),2) AS monthly_delta_avg_materiality_adj_insight,
    ROUND((ZEROIFNULL(
        AVG(dpd.all_categories_adj_insight) - LAG(AVG(dpd.all_categories_adj_insight)) OVER (
            PARTITION BY dpd.ticker, dpd.portfolio ORDER BY TO_VARCHAR(dpd.date, 'yyyy-mm')
        ))),2) AS monthly_delta_all_categories_adj_insight,
    ROUND((ZEROIFNULL(
    AVG(dpd.internal_esg_score) - LAG(AVG(dpd.internal_esg_score)) OVER (
        PARTITION BY dpd.ticker, dpd.portfolio ORDER BY TO_VARCHAR(dpd.date, 'yyyy-mm')
    ))),2) AS monthly_delta_internal_esg_score,
    ROUND(AVG(dpd.total_shares)) AS avg_monthly_total_shares,
    ROUND(MAX(dpd.total_shares),2) AS max_monthly_total_shares,
    ROUND(MIN(dpd.total_shares),2) AS min_monthly_total_shares,
    ROUND(SUM(dpd.change_in_shares),2) AS monthly_change_in_shares
FROM daily_portfolio_detail_v dpd
WHERE 1=1
GROUP BY year_month, dpd.ticker, dpd.security_name, dpd.portfolio
ORDER BY year_month DESC, dpd.ticker, dpd.portfolio
)
SELECT 
    mpd.year_month,
    mpd.portfolio,
    mpd.ticker,
    mpd.security_name,
    mpd.monthly_delta_all_categories_adj_insight,
    mpd.monthly_delta_avg_materiality_adj_insight,
    mpd.monthly_delta_internal_esg_score,
    mpd.avg_monthly_total_shares,
    mpd.min_monthly_total_shares,
    mpd.max_monthly_total_shares,
    mpd.monthly_change_in_shares
FROM _monthly_portfolio_delta mpd
WHERE 1=1
    AND mpd.monthly_delta_all_categories_adj_insight < 0
    AND mpd.monthly_delta_avg_materiality_adj_insight < 0
    AND mpd.monthly_delta_internal_esg_score < 0
    AND year_month = '2022-03'
ORDER BY 
    year_month DESC, 
    mpd.monthly_delta_all_categories_adj_insight,
    mpd.monthly_delta_avg_materiality_adj_insight;
   

-- Cerus Corporate was the top decliner across all ESG scores in March 2022
-- let's leverage another Snowflake Marketplace Provider: Accern 
-- to deep dive into CERS recent 10k ESG metrics

    /*--- 
     Data Marketplace Click Path: If you would like to show the
        listings from Accern in the Marketplace please follow below:
            • Snowsight -> Data Marketplace -> Search: Accern
    ---*/
    
SELECT 
    esg.entity_ticker AS ticker,
    esg.entity_name,
    esg.entity_sector,
    ROUND(esg.entity_sentiment,2) as entity_sentiment,
    esg.event_hits,
    esg.event_text
FROM frostbyte_esg.analytics.russell3000_10k_esg_v esg
WHERE 1=1
    AND esg.primary_signal = 'TRUE'
    AND esg.entity_ticker = 'CERS'
    AND esg.event = 'Social - Product Quality & Safety'
ORDER BY esg.harvested_at DESC, esg.entity_sentiment DESC;

-- set up the BI tables
