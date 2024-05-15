#Report task1

Набор данных составляет 40 Гб, полученный путем многократного объединения одной и той же таблицы в pandas из исходного набора данных. Затем он был загружен на hdfs.

В этом задании мы начали с настройки Hadoop, hive, derby и hive. Затем мы загрузили данные bi из kaggle, доступ к которым можно получить по следующей ссылке
https://www.kaggle.com/datasets/therohk/india-headlines-news-dataset. Поскольку название было немного длиннее, мы сократили его, чтобы упростить использование. Мы переименовали набор данных в india-news-headlines.csv. Теперь, когда у нас был набор данных, пришло время загрузить его в Hadoop. Мы создали каталог в Hadoop с помощью командной строки, используя hdfs dfs -mkdir /data. Это должна была быть наша рабочая директория для доступа к данным и работы с ними. После того как это было успешно сделано (об этом свидетельствуют кластеры на рисунке 1.1), мы загрузили загруженный набор данных в Hadoop (об этом свидетельствует рисунок 1.2).

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/552c6ea0-99af-4e9b-8125-19a85f1e2796)

Рисунок 1.1

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/8270942b-c5ca-486b-9080-6ac19fa93407)

Рисунок 1.2

Теперь, когда мы загрузили данные, появилась возможность завершить задачу. В hive мы создали базу данных с именем news с помощью команды CREATE DATABASE news; далее мы создали внешнюю таблицу, указывающую на Hadoop, с помощью следующего SQL-скрипта

CREATE EXTERNAL TABLE india_news_headlines (  
    publish_date STRING,  
    headline_category STRING,  
    headline_text STRING  
)  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY ','  
LOCATION '/data/';  

Затем мы загрузили данные в таблицу из Hadoop, используя следующее  
LOAD DATA INPATH '/data/india-news-headlines.csv' INTO TABLE india_news_headlines;  
Чтобы убедиться, что данные присутствуют;  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/435883d6-c1ce-404c-9efd-2197becd0a1c)

Теперь, когда данные были готовы, самое время запустить различные SQL-операции с данными, чтобы проанализировать их производительность. Были выполнены следующие сценарии;
-- Использование условия WITH для временных вычислений

WITH temp_table AS (  
    SELECT publish_date, headline_category, headline_text  
    FROM india_news_headlines  
    WHERE publish_date BETWEEN '20010102' AND '20010110'  
)  
-- Use GROUP BY to aggregate data  
SELECT headline_category, COUNT(*) AS num_headlines  
FROM temp_table  
GROUP BY headline_category  
ORDER BY num_headlines DESC  
LIMIT 10; -- Use LIMIT to restrict the number of rows returned  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/6272772a-5b4f-4716-8877-fc2a31d14ac0)


To measure the performance we ran the following query  
EXPLAIN  
WITH temp_table AS (  
    SELECT publish_date, headline_category, headline_text  
    FROM india_news_headlines  
    WHERE publish_date BETWEEN '20010102' AND '20010110'  
)  
SELECT headline_category, COUNT(*) AS num_headlines  
FROM temp_table  
GROUP BY headline_category  
ORDER BY num_headlines DESC  
LIMIT 10;  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/c4ef173b-27d9-40d8-9556-3428bf5f4687)


![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/f904f115-d3c8-4ac2-9188-bcc7c70d4b28)


Время выполнения запроса составляет 0,434 секунды. Это время включает в себя время, затраченное на планирование, выполнение задания MapReduce и получение результатов.
