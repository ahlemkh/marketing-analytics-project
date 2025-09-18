-- Query to clean whitespace issues in the ReviewText column

SELECT 
    ReviewID,
    CustomerID,
    ProductID,
    ReviewDate,
    rating,
    REPLACE(ReviewText, '  ', ' ') AS ReviewText
FROM
    marketing_analytics.`dbo.customer_reviews`;