from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, expr, when
from pyspark.sql.types import FloatType, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("User Rechnung Processing") \
    .config("spark.driver.memory", "16g").getOrCreate()

# Define the CSV folder and filenames
CSV_FOLDER = '/Volumes/MARAL/CSV/F01/'
V2AD1001 = 'V2AD1001'
V2AD1056 = 'V2AD1056'
V2AD1156 = 'V2AD1156'

# Load CSV files into DataFrames
V2AD1001_df = spark.read.csv(f'{CSV_FOLDER}{V2AD1001}.csv', header=True, sep=';')
V2AD1056_df = spark.read.csv(f'{CSV_FOLDER}{V2AD1056}.csv', header=True, sep=';')
V2AD1156_df = spark.read.csv(f'{CSV_FOLDER}{V2AD1156}.csv', header=True, sep=';')

# Select specific columns
V2AD1056_df_edited = V2AD1056_df.select("VERWEIS", "HERKUNFT", "AUFTRAG_NR", "RECH_NR", "DATUM", "AUF_ANLAGE", "BEST_WERT", "MEDIACODE")
V2AD1156_df_edited = V2AD1156_df.select("RECHNUNG", "ART_NR", "GROESSE", "FARBE", "BEZEICHNG", "PREIS", "MENGE")
V2AD1001_df_edited = V2AD1001_df.select("NUMMER", "ANREDE", "VORNAME", "NAME", "QUELLE", "GEBURT", "ORT", "SYS_ANLAGE")

# Extract substring from the 'VERWEIS' column and clean up 'NUMMER'
V2AD1056_df_edited = V2AD1056_df_edited.withColumn('NUMMER', substring('VERWEIS', 3, 10).cast(StringType())) \
    .withColumn('NUMMER', expr("lpad(NUMMER, 10, '0')"))

# Clean up 'RECH_NR' column
V2AD1056_df_edited = V2AD1056_df_edited.withColumn('RECH_NR', col('RECH_NR').cast(StringType())) \
    .withColumn('RECH_NR', expr("lpad(RECH_NR, 12, '0')"))

# Ensure 'NUMMER' is consistent across all DataFrames
V2AD1001_df_edited = V2AD1001_df_edited.withColumn('NUMMER', col('NUMMER').cast(StringType())) \
    .withColumn('NUMMER', expr("lpad(NUMMER, 10, '0')"))

V2AD1156_df_edited = V2AD1156_df_edited.withColumn('RECHNUNG', col('RECHNUNG').cast(StringType())) \
    .withColumn('RECHNUNG', expr("lpad(RECHNUNG, 12, '0')"))

# Join the DataFrames
rechnung = V2AD1156_df_edited.join(V2AD1056_df_edited, V2AD1156_df_edited.RECHNUNG == V2AD1056_df_edited.RECH_NR, 'inner')
user_rechnung = rechnung.join(V2AD1001_df_edited, 'NUMMER', 'inner')

# Final selection of columns
user_rechnung = user_rechnung.select("VERWEIS", "AUFTRAG_NR", "NUMMER", "ANREDE", "VORNAME", "NAME", "BEST_WERT", "DATUM", "GEBURT", "ORT",
                                     "RECHNUNG", "ART_NR", "SYS_ANLAGE", "GROESSE", "FARBE", "BEZEICHNG", "PREIS", "MENGE", "HERKUNFT", "MEDIACODE")

# Convert numeric columns
user_rechnung = user_rechnung.withColumn('MENGE', col('MENGE').cast(FloatType())) \
    .withColumn('PREIS', col('PREIS').cast(FloatType())) \
    .withColumn('BEST_WERT', col('BEST_WERT').cast(FloatType()))

# Convert date columns
user_rechnung = user_rechnung.withColumn('SYS_ANLAGE', col('SYS_ANLAGE').cast('timestamp')) \
    .withColumn('DATUM', col('DATUM').cast('timestamp')) \
    .withColumn('GEBURT', col('GEBURT').cast('timestamp'))

# Filtering based on 'ART_NR' and 'BEZEICHNG'
user_rechnung = user_rechnung.filter(~col('ART_NR').isin(['VK', 'UN', '1', '99H99', '99H99P', '01A01', '982M08A', '029Z15G', '029Z15D', '086L07R', '309H01SE',
                                                          '.', '0', '99HA99', '99HA9', '99H999', '99H9', '09WV2', '99B999', '04H20', '01K01', '99M99'])) \
    .filter(~col('ART_NR').rlike(r'^[VRFIMJPSTKLEABZWUHDCL]')) \
    .filter(~col('ART_NR').rlike(r'[EVSGDRPCRSFHIJ]$')) \
    .filter(~col('BEZEICHNG').rlike(r'(?i)Beilage|Anleit|Versand|Etikett|Katalog|Expresskosten|Pfandberechnung|rezept'))



# Mapping 'ANREDE' and 'HERKUNFT'
anrede_map = {'1': 'Herrn', '2': 'Frau', '3': 'Frau/Herr', '4': 'Firma', '5': 'Leer(Firmenadresse)', '6': 'Fräulein', '7': 'Familie', 'X': 'Divers'}
herkunft_map = {'1': 'Schriftlich', '2': 'Fax', '3': 'Telefon', '4': 'Internet', '5': 'Call-Center', '6': 'Ladenverkauf', '7': 'Vertreter', 
                '8': 'E-Mail', '9': 'Anrufbeantworter/Mailbox', 'B': 'Beleglesung', 'E': 'Marktplätze', 'F': 'Amazon-Fulfillment', 'M': 'Messe', 'S': 'SMS'}

# Apply the mapping using when and otherwise
for key, value in anrede_map.items():
    user_rechnung = user_rechnung.withColumn('ANREDE', 
                                             when(col('ANREDE') == key, value).otherwise(col('ANREDE')))

for key, value in herkunft_map.items():
    user_rechnung = user_rechnung.withColumn('HERKUNFT', 
                                             when(col('HERKUNFT') == key, value).otherwise(col('HERKUNFT')))
df_single_partition = user_rechnung.repartition(1) 

user_rechnung.coalesce(1).write.csv('user_rechnungen.csv', header=True, sep=';', mode='overwrite')
# df_single_partition.write.parquet('user_rechnungen.parquet',  mode='overwrite')
