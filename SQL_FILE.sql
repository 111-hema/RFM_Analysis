
create table dataa_table(  ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float ,   
DATE datetime,
ORDERLINENUMBER float	,
SALES	float,
STATUS varchar(20),
QTR_ID float,
MONTH_ID int	,
YEAR_ID float	,
PRODUCTLINE varchar(20),
MSRP float	,
PRODUCTCODE	varchar(20)	,
PRODUCTNAME	varchar(40),
ADDRESSLINE1 varchar(100),
CITY  varchar(20)	,
STATE  varchar(20),
POSTALCODE varchar(20)	,
COUNTRY	varchar(10)	,
TERRITORY varchar(10) ,
FULLNAME varchar(40),
DEALSIZE varchar(10)); 

-- Cleaning the data

create temporary table temp_Sales_data as
select ORDERNUMBER,	QUANTITYORDERED, 	PRICEEACH	, DATE	,ORDERLINENUMBER	, SALES,	STATUS,	QTR_ID,	MONTH_ID	,YEAR_ID,	PRODUCTLINE	,MSRP	,PRODUCTCODE	,PRODUCTNAME,	ADDRESSLINE1,	CITY,	STATE,	POSTALCODE,	COUNTRY	,TERRITORY	,FULLNAME, 	DEALSIZE  from
( select *, row_number() over (partition by ORDERNUMBER,	QUANTITYORDERED, 	PRICEEACH	, DATE	,ORDERLINENUMBER	, SALES,	STATUS,	QTR_ID,	MONTH_ID	,YEAR_ID,	PRODUCTLINE	,MSRP	,PRODUCTCODE	,PRODUCTNAME,	ADDRESSLINE1,	CITY,	STATE,	POSTALCODE,	COUNTRY	,TERRITORY	,FULLNAME, 	DEALSIZE order by ordernumber) as row_data 
from data_table) data1 
where row_data = 1;

select * from temp_Sales_data;

create temporary table sales_Dataa as
select * from temp_Sales_data where status !="NA" and territory!="Unknown" and postalcode !="NA" ;

select * from sales_Dataa;

-- Checking Unique Values --

select distinct year_id from sales_Dataa;
select distinct country from sales_Dataa;
select distinct status from sales_Dataa;
select distinct productline from sales_Dataa;
select distinct dealsize from sales_Dataa;
select distinct territory from sales_Dataa;

-- Starting the Analysis--

-- Analysing highest Revenue w.r.t different attributes-- 
select round(Sum(Sales),2) as Revenue , Productline from sales_Dataa group by productline order by 1 desc;
select round(Sum(Sales),2) as Revenue , year_id from sales_Dataa group by year_id order by 1 desc;
select round(Sum(Sales),2) as Revenue , dealsize from sales_Dataa group by dealsize order by 1 desc;

-- Obtaining  month which generated highest revenue for respective year-- 

select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency from sales_Dataa where year_id = "2003" group by month_id order by 1 desc;
select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency from sales_Dataa where year_id = "2004" group by month_id order by 1 desc;
select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency from sales_Dataa where year_id = "2005" group by month_id order by 1 desc;

-- Idenfying Products sold in highest sale period -- 

select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency, productline from sales_Dataa where year_id = "2003" and month_id = "11" group by month_id, productline order by 1 desc;

select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency, productline from sales_Dataa where year_id = "2004" and month_id = "11" group by month_id, productline order by 1 desc;

select round(Sum(Sales),2) as Revenue , month_id, count(ordernumber) as frequency, productline from sales_Dataa where year_id = "2005" and month_id = "5" group by month_id, productline order by 1 desc;

-- Analysing best customers with RFM Analysis--

-- Store max(date) in a variable
SET @max_ordered_date = (SELECT MAX(date) FROM sales_dataa);

-- Use the variable in the main query

CREATE TABLE rfm AS
SELECT 
    c.*, 
    rfm_recency + rfm_frequency + rfm_monetary AS rfm,
    CONCAT(CAST(rfm_recency AS CHAR), CAST(rfm_frequency AS CHAR), CAST(rfm_monetary AS CHAR)) AS rfm_Value
FROM (
    WITH rfm AS (
        SELECT 
            fullname, 
            SUM(sales) AS MonetaryValue, 
            AVG(sales) AS AvgMonetaryValue, 
            COUNT(ordernumber) AS Frequency, 
            MAX(date) AS last_ordered_Date, 
            @max_ordered_date AS max_ordered_date,
            DATEDIFF(@max_ordered_date, MAX(date)) AS recency
        FROM 
            sales_dataa
        GROUP BY 
            fullname
    ),
    rfm_calc AS (
        SELECT 
            r.*, 
            NTILE(4) OVER (ORDER BY recency) AS rfm_recency,
            NTILE(4) OVER (ORDER BY Frequency desc) AS rfm_frequency,
            NTILE(4) OVER (ORDER BY MonetaryValue desc) AS rfm_monetary
        FROM rfm r
    )
    SELECT * FROM rfm_calc
) AS c;  

select * from rfm order by recency;

select fullname, rfm_recency , rfm_frequency , rfm_monetary,
case 
        -- Loyal Customers (Highest recency, recent activity)
        WHEN rfm_value IN (111, 112, 113, 114, 121, 122, 123, 124, 131, 132) THEN 'Loyal Customers'
        
        -- Slipping Away (Moderate recency, moderate engagement)
        WHEN rfm_value IN (223, 224, 233, 234, 243, 244, 323, 324, 333, 334) THEN 'Slipping Away'
        
        -- Potential Churners (Low recency, fading engagement)
        WHEN rfm_value IN (311, 312, 321, 322, 331, 332, 411, 412, 421, 422) THEN 'Potential Churners'
        
        -- Lost Customers (Longest inactivity, disengaged)
        WHEN rfm_value IN (433, 434, 443, 444, 341, 342, 431, 432) THEN 'Lost Customers'
        
        ELSE 'Other'
    END AS CustomerAnalysis
FROM 
    rfm;

