# ---------------------------------------------------------------
# Phase 3: Advanced Sentiment Analysis with Python
# Project: Marketing Analytics Business Case
#
# Objective:
# ShopEasy, an online retail business, is experiencing reduced
# customer engagement and conversion rates despite heavy investment
# in online marketing campaigns. This analysis leverages customer
# review data to extract insights into customer sentiment, aiming
# to identify areas for improvement in marketing strategies.
#
# Key Business Challenges:
# - Declining customer engagement with site and marketing content
# - Decreasing conversion rates (visitors → customers)
# - High marketing expenses with low returns
# - Need for structured customer feedback analysis
#
# Approach (this phase):
# 1. Load customer reviews data from SQL
# 2. Apply sentiment analysis using NLTK VADER
# 3. Categorize sentiment by combining sentiment score with product ratings
# 4. Bucket sentiment scores into ranges for easier interpretation
# 5. Save enriched dataset for further business analysis
# ---------------------------------------------------------------

import pandas as pd
import pyodbc
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sqlalchemy import create_engine

# Download the VADER lexicon for sentiment analysis if not already present
nltk.download('vader_lexicon')


# ---------------------------------------------------------------
# Data Loading Functions
# ---------------------------------------------------------------

def fetch_data_from_sql():
    """
    Fetch customer reviews data from SQL Server using SQLAlchemy.
    Returns:
        pd.DataFrame: DataFrame containing customer reviews.
    """
    engine = create_engine(
        "mysql+mysqlconnector://<USERNAME>:<PASSWORD>@<HOST>/<DATABASE>"
    )

    query = """
        SELECT ReviewID, CustomerID, ProductID, ReviewDate, Rating, ReviewText
        FROM marketing_analytics.`dbo.customer_reviews`
    """
    df = pd.read_sql(query, engine)
    return df


def fetch_data_from_sql_pyodbc():
    """
    Alternative method to fetch customer reviews using pyodbc.
    This approach uses a direct cursor for database interaction.
    Returns:
        pd.DataFrame: DataFrame containing customer reviews.
    """
    conn = pyodbc.connect(
        "Driver={MySQL ODBC 9.4 Unicode Driver};"
        "Server=localhost;"
        "Database=marketing_analytics;"
        "Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    query = """
        SELECT ReviewID, CustomerID, ProductID, ReviewDate, Rating, ReviewText
        FROM marketing_analytics.`dbo.customer_reviews`
    """
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df


# Load customer reviews into DataFrame
customer_reviews = fetch_data_from_sql()


# ---------------------------------------------------------------
# Sentiment Analysis Functions
# ---------------------------------------------------------------

# Initialize the VADER sentiment analyzer
sia = SentimentIntensityAnalyzer()


def calculate_score(review):
    """
    Compute the compound sentiment score for a given review text
    using NLTK VADER sentiment analyzer.
    Args:
        review (str): Review text
    Returns:
        float: Compound sentiment score between -1.0 (negative) and +1.0 (positive)
    """
    sentiment = sia.polarity_scores(review)
    return sentiment["compound"]


def categorize_sentiment(score, rating):
    """
    Categorize sentiment by combining text-based sentiment score
    with the numerical product rating.
    Args:
        score (float): Compound sentiment score
        rating (int): Product rating (1–5)
    Returns:
        str: Sentiment category (Positive, Negative, Mixed, Neutral)
    """
    if score > 0.05:  # Positive sentiment
        if rating >= 4:
            return 'Positive'
        elif rating == 3:
            return 'Mixed Positive'
        else:
            return 'Mixed Negative'
    elif score < -0.05:  # Negative sentiment
        if rating <= 2:
            return 'Negative'
        elif rating == 3:
            return 'Mixed Negative'
        else:
            return 'Mixed Positive'
    else:  # Neutral sentiment
        if rating >= 4:
            return 'Positive'
        elif rating <= 2:
            return 'Negative'
        else:
            return 'Neutral'


def sentiment_bucket(score):
    """
    Bucket sentiment scores into defined ranges for interpretability.
    Args:
        score (float): Compound sentiment score
    Returns:
        str: Bucket label
    """
    if score >= 0.5:
        return '0.5 to 1.0'      # Strongly positive
    elif 0.0 <= score < 0.5:
        return '0.0 to 0.49'     # Mildly positive
    elif -0.5 <= score < 0.0:
        return '-0.49 to 0.0'    # Mildly negative
    else:
        return '-1.0 to -0.5'    # Strongly negative


# ---------------------------------------------------------------
# Apply Sentiment Analysis
# ---------------------------------------------------------------

# Calculate sentiment scores from review text
customer_reviews["SentimentScore"] = customer_reviews['ReviewText'].apply(calculate_score)

# Assign sentiment categories using both sentiment score and rating
customer_reviews['SentimentCategory'] = customer_reviews.apply(
    lambda row: categorize_sentiment(row['SentimentScore'], row['Rating']), axis=1
)

# Assign sentiment buckets based on sentiment score ranges
customer_reviews['SentimentBucket'] = customer_reviews['SentimentScore'].apply(sentiment_bucket)


# ---------------------------------------------------------------
# Save Results
# ---------------------------------------------------------------

# Display sample of positive sentiment reviews
print(customer_reviews[customer_reviews["SentimentScore"] > 0])

# Save enriched DataFrame with sentiment analysis to CSV
customer_reviews.to_csv('customer_reviews_with_sentiment.csv', index=False)
