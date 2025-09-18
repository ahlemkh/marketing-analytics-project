-- Query to clean and normalize the engagement_data table

SELECT 
    EngagementID,
    ContentID,
    CampaignID,
    UPPER(ContentType) AS ContentType,
    SUBSTRING_INDEX(ViewsClicksCombined, '-', 1) AS views,
    SUBSTRING_INDEX(ViewsClicksCombined, '-', - 1) AS clicks,
    likes,
    DATE_FORMAT(EngagementDate, '%d-%m-%Y') AS EngagementDate
FROM
    marketing_analytics.`dbo.engagement_data`;