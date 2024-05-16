#1.Основные выполненные функции  
· Загрузка набора данных из HDFS и преобразование его в Spark DataFrame.  
· Уменьшение веса DataFrame.  
·Подсчитайте количество новостей за год и найдите 10 лет с наибольшим количеством новостей.  
· Отфильтруйте заголовки новостей в определенной категории ("история").  
· Извлеките слова из заголовков и отфильтруйте деактивированные слова.  
· Подсчитайте частоту слов и найдите 10 наиболее часто встречающихся слов.  

#2.Как выполнять вышеперечисленные функции

##1.Инициализация SparkSession
Используйте модуль SparkSession.builder для создания и инициализации сессии Spark под названием "Task2". Эта сессия обеспечивает среду и управление ресурсами для последующих операций Spark. 2.

##2.Загрузка набора данных из HDFS
Используйте метод spark.read.option("header", True).csv() для загрузки CSV-файла с именем "india-news-headlines.csv" из файловой системы HDFS и преобразования его в Spark DataFrame. Параметр option("header", True) указывает Spark на загрузку набора данных из файловой системы HDFS. Параметр option("header", True) указывает Spark, что CSV-файл содержит строку заголовка. 3.

##3.Удаление дубликатов кадров данных
Поскольку исходные данные могут содержать дубликаты строк, программа использует метод DataFrame dropDuplicates () для удаления дубликатов строк, создавая новый DataFrame, не содержащий дубликатов данных. Затем DataFrame будет преобразован в RDD.

##4.Извлечение года из заголовков новостей
Программа извлекает год (первые 4 символа) из каждого новостного заголовка с помощью функции преобразования map() в RDD и связывает его со значением count 1 для создания нового RDD, где каждый элемент представляет собой кортеж в формате (year, 1). Затем программа преобразует RDD в новый DataFrame, содержащий два столбца "Year" и "Count".

<img width="428" alt="image" src="https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/442ceab8-bee8-4763-8e6f-95535810014c">

##5. Статистика по количеству новостей в год
Используйте метод reduceByKey() в RDD для подсчета количества новостей за год. Метод будет соответствовать году общего количества значений, чтобы сгенерировать новый RDD, где каждый элемент представляет собой год и соответствующее ему количество новостей. Процедура использования метода sortBy() в соответствии со значением count в порядке убывания RDD и отбора первых 10 записей

<img width="600" alt="image" src="https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/0e9d929d-cad9-4f77-9359-b40ffe69f900">

##6. Фильтрация определенных категорий данных
Определите коллекцию деактивированных слов и укажите категорию для фильтрации как "история". С помощью метода filter() RDD отфильтруйте заголовки новостей с категорией "история" из исходного RDD и сгенерируйте новый RDD.

<img width="495" alt="image" src="https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/fb72ed69-4181-449b-864e-481e867d8375">

##7. Извлечение слов из заголовков новостей
Используйте метод flatMap() в RDD, чтобы разбить каждый новостной заголовок на слова и связать их с соответствующей категорией для создания нового RDD, а также используйте метод filter(), чтобы отфильтровать деактивированные слова для создания другого нового RDD.

##8. статистика частоты слов
Используйте методы map() и reduceByKey() RDD для подсчета частоты встречаемости каждого слова. Метод map() будет ассоциирован с каждым словом со значением count 1. Метод reduceByKey() будет суммировать значения count одного и того же слова, чтобы получить общее количество вхождений каждого слова. Процедура использует метод sortBy() для сортировки RDD по убыванию значений счетчика и берет первые 10 записей, чтобы получить 10 слов с наибольшей частотой.

<img width="545" alt="image" src="https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/7b991bed-c5d1-4a03-87b3-e0fad19668a8">


#Этапы сборки окружения и отчеты.  
Сначала создайте виртуальную среду conda  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/f078b09b-8e2b-4e0c-bc8d-5df067d12b8d)  

Как показано на рисунке, создание среды завершено  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/89a23d16-cdf0-4ad9-886f-1f694ebd2214)  

Коммутационные среды  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/38b27626-6677-492d-8d1b-b99971eb0357)  

Установка pyspark  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/6c894b96-c847-412b-bce8-c783b55b8cb8)  

Завершите установку pyspark  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/17a4449e-d356-4e2f-93c7-d59eb2713515)  

Установка pandas  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/71efb384-e485-4907-87ad-8e54f9ee432d)  

Установите matplotlib  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/4706b42c-5c91-477f-abb7-a134414ddb1c)  

Установка jupyter  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/aab7fbef-9643-48ab-b17d-99b0552d0749)  

Запустите jupyter  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/979493ba-ad84-4e93-8648-76446de4ce23)  

Запустите код. Экспорт информации о конфигурации среды  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/5c86b31b-d719-4dc5-87f5-afd71706c7c5)  

Упаковка общей информации о виртуальной среде  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/edbff040-92d8-4c11-836f-f9f9e3c788ee)  

![image](https://github.com/gyt0308/Technologies-for-storing-and-processing-large-amounts-of-data/assets/118203672/15bec2c4-df99-4111-8061-7e9ee117fe89)

ссылка на архив:https://drive.google.com/drive/folders/1UUcaELQldl2qA_oiIzbcA9ccDLRnc-3G?usp=sharing











