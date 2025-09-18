-- Common Table Expression (CTE) to identify and tag duplicate records
with duplicate_cte as (
SELECT 
    JourneyID, CustomerID, ProductID, Stage, Action,
    Duration, row_number() over( partition by CustomerID, ProductID, VisitDate, Stage, Action 
    order by JourneyID) as row_num
FROM marketing_analytics.`dbo.customer_journey`)

-- Select all records from the CTE where row_num > 1, which indicates duplicate entries
select * from duplicate_cte 
    where row_num > 1
    order by JourneyID;
    
    
-- Outer query selects the final cleaned and standardized data
    
   select 
   JourneyID,
    CustomerID,
    ProductID,
    VisitDate,
    Stage,
    Action,
    COALESCE(nullif(Duration,''), AvgDuration) as Duration -- Replaces missing durations with the average duration for the corresponding date
    from (SELECT 
    JourneyID,
    CustomerID,
    ProductID,
    VisitDate,
    upper (Stage) as Stage,
    Action,
    Duration, 
    avg(Duration) over (partition by VisitDate ) AS AvgDuration,
    row_number() over( partition by CustomerID, ProductID, VisitDate, Stage, Action 
    order by JourneyID) as row_num
FROM marketing_analytics.`dbo.customer_journey` ) as subquery
where row_num =1 ;



 