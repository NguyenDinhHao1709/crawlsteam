from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, udf
from pyspark.sql.types import StringType
import re

# Đường dẫn đến tệp JAR driver PostgreSQL
driver_path = "C:\\Users\\Hao\\Downloads\\postgresql-42.7.4.jar"


# Khởi tạo SparkSession với MongoDB Spark Connector
spark = SparkSession.builder \
    .appName("Game Data Processing") \
    .config("spark.jars", driver_path) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.driver.extraClassPath", driver_path) \
    .config("spark.executor.extraClassPath", driver_path) \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/DATASTEAM.games") \
    .load()

# In danh sách các cột trong DataFrame
print("Danh sách cột trong DataFrame:", df.columns)

# Làm sạch các giá trị như "or better", "or over", "or", "()", "," và "/"
df = df.withColumn('cleaned_cpu', regexp_replace(col('minimum_cpu'), r'\(.*?\)|or better|or over|or|,|\/|\\', ''))

# Loại bỏ khoảng trắng thừa
df = df.withColumn('cleaned_cpu', trim(col('cleaned_cpu')))

# Định nghĩa UDF để chuẩn hóa tên CPU (Intel Core, AMD FX, AMD Ryzen)
def normalize_cpu(cpu_string):
    cpu_string = cpu_string.lower()
    cpu_string = re.sub(r'intel\s*core\s*i\s*([3579])', r'Intel Core i\1', cpu_string, flags=re.IGNORECASE)
    cpu_string = re.sub(r'amd\s*fx\s*-?\s*(\d+)', r'AMD FX-\1', cpu_string, flags=re.IGNORECASE)
    cpu_string = re.sub(r'amd\s*ryzen\s*(\d+)', r'AMD Ryzen \1', cpu_string, flags=re.IGNORECASE)
    return cpu_string

# Tạo UDF với Spark
normalize_cpu_udf = udf(normalize_cpu, StringType())

# Áp dụng UDF vào cột 'cleaned_cpu'
df = df.withColumn('cleaned_cpu', normalize_cpu_udf(col('cleaned_cpu')))

# Xử lý cột _id (chuyển ObjectId sang chuỗi)
df = df.withColumn("_id", col("_id.oid").cast("string"))

print(f"Số hàng trong DataFrame: {df.count()}")
# Hiển thị 5 hàng đầu tiên
df.show(5)

# Lưu DataFrame vào PostgreSQL
try:
    # Lưu DataFrame vào PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/datasteam") \
        .option("dbtable", "data_steam") \
        .option("user", "postgres") \
        .option("password", "Hao170909") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Dữ liệu đã được lưu thành công vào PostgreSQL.")
except Exception as e:
    print("Đã xảy ra lỗi khi lưu dữ liệu vào PostgreSQL:", e)
