# **Инструкция по выполнению Практического задания №4 с использованием Apache Spark на YARN**

Данная инструкция пошагово описывает выполнение Практического задания №4, связанного с использованием Apache Spark под управлением YARN для чтения, трансформации и записи данных. Инструкция включает настройку, выполнение скриптов, подключение к HDFS и автоматизацию процесса обработки данных.


### **Шаг 1: Подготовка**
1. **Установка Apache Spark:**
   В `spark-env.sh` настроим переменные окружения для работы с YARN:
     ```bash
     export SPARK_HOME=/path/to/spark
     export PATH=$SPARK_HOME/bin:$PATH
     export HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop
     ```

2. **Проверка доступность Spark на YARN:**
     ```bash
     spark-submit --master yarn --deploy-mode client --help
     ```
     
3. **3.Скачать Scala**
     ```bash
     sudo apt install scala
     ```

### **Шаг 2: Загрузка скрипта на сервер**

1. Сохраним скрипт с именем `hw4.scala`.（Скрипт с комментариями находится в отдельном файле в репозитории）

2. Используем `scp` для загрузки файла скрипта на сервер:
   ```bash
   scp hw4.scala team@176.109.91.12:/home/team/
   ```

3. Проверяем успешность загрузки, подключившись к серверу:
   ```bash
   ls /home/team/hw4.scala
   ```

4. Загрузим Bash-скрипт:
   ```bash
   scp hw4.sh team@176.109.91.12:/home/team/
   ```


### **Шаг 3: Компиляция и упаковка Scala-скрипта**

1. Подключим к серверу:
   ```bash
   ssh team@176.109.91.12
   ```

2. Переидем в директорию, куда был загружен скрипт:
   ```bash
   cd /home/team
   ```

3. Скомпилируем и запустим скрипт с помощью `spark-submit`:
     ```bash
     spark-submit --class hw4 --master yarn /home/team/hw4.scala
     ```

### **Шаг 4: Выполнение скрипта**

1. Перед запуском скрипта уточним, что пути HDFS `/input` и `/output` существуют, или создать их:
   ```bash
   hdfs dfs -mkdir -p /input
   hdfs dfs -mkdir -p /output
   ```

2. Загрузим входные данные в HDFS:
   ```bash
   hdfs dfs -put /path/to/local/input.txt /input/
   ```

3. Запустим Bash-скрипт:
   ```bash
   bash run_hw4.sh
   ```

### **Шаг 5: Проверка выполнения**

1. Проверьте выходные данные в HDFS:
   ```bash
   hdfs dfs -ls /output/wordcount_partitioned
   ```

2. Опционально просмотрите обработанные данные:
   ```bash
   hdfs dfs -cat /output/wordcount_partitioned/part-*
   ```

### **Авторизация**

Bash-скрипта (`run_hw4.sh`), автоматизирующего выполнение задания:

```bash
#!/bin/bash

# Устанавливаем переменные окружения для Hadoop и Spark
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

INPUT_PATH="/input/input.txt"
OUTPUT_PATH="/output/wordcount_partitioned"

echo "Подготовка директорий в HDFS..."

# Создаем директории в HDFS, если они не существуют
hdfs dfs -test -e $INPUT_PATH || hdfs dfs -mkdir -p /input
hdfs dfs -test -e $OUTPUT_PATH && hdfs dfs -rm -r $OUTPUT_PATH

echo "Загрузка данных в HDFS..."
# Загружаем входные данные в HDFS
hdfs dfs -put -f /home/team/input.txt $INPUT_PATH

# !!!!!! ЗДЕСЬ НАЧИНАЕТСЯ ВЫПОЛНЕНИЕ SCALA СКРИПТА !!!!!!!
echo "Запуск Spark..."
spark-submit --class hw4 --master yarn /home/team/hw4.jar

echo "Задание завершено. Проверка выходных данных..."

# Проверяем выходные данные
hdfs dfs -ls $OUTPUT_PATH
hdfs dfs -cat $OUTPUT_PATH/part-*

echo "Все выполнено!"
```
