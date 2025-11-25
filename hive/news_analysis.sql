CREATE DATABASE IF NOT EXISTS news_analysis;
USE news_analysis;

CREATE TABLE IF NOT EXISTS fact_news (
    category STRING,     
    clean_content STRING,  
    word_count INT        
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS dim_category (
    category STRING,       
    category_code INT,     
    category_desc STRING  
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/input/news/cleaned_news.txt' OVERWRITE INTO TABLE fact_news;

INSERT INTO dim_category VALUES 
('体育', 1, 'Sports news'),
('财经', 2, 'Finance news'),
('房产', 3, 'Real estate news'),
('家居', 4, 'Home news'),
('教育', 5, 'Education news'),
('科技', 6, 'Technology news'),
('时尚', 7, 'Fashion news'),
('时政', 8, 'Politics news'),
('游戏', 9, 'Game news'),
('娱乐', 10, 'Entertainment news');


SELECT dc.category_desc, COUNT(1) AS doc_count 
FROM fact_news fn
JOIN dim_category dc ON fn.category = dc.category
GROUP BY dc.category_desc
ORDER BY doc_count DESC;

SELECT category, AVG(word_count) AS avg_word_count 
FROM fact_news
GROUP BY category
ORDER BY avg_word_count DESC;

SELECT category, word_count, SUBSTR(clean_content, 1, 50) AS content_sample 
FROM fact_news
ORDER BY word_count DESC
LIMIT 10;