from pyspark.sql import SparkSession


# Step 1: Initialize SparkSession with Hive support

spark = SparkSession.builder \
    .appName("HiveReadExample") \
    .config("spark.sql.warehouse.dir", "/tmp/nicksteen/hive_ass") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("SELECT customer_id, amount FROM loans_nick WHERE amount > 10000")
df.show()
df.write.mode("overwrite").saveAsTable("default.high_value_loans")


df = spark.sql("SELECT customer_id, amount FROM loans_nick WHERE amount < 5000")
df.show()
df.write.mode("overwrite").saveAsTable("default.low_value_loans")



df = spark.sql("SELECT l.loan_id, l.amount, lp2.due_date, lp2.status FROM loan_payments2_nick lp2 inner join loans_nick l on l.loan_id = lp2.loan_id WHERE lp2.status='late'")
df.show()
df.write.mode("overwrite").saveAsTable("default.late_loans_nick")


df = spark.sql("SELECT lt.type_name, SUM(l.amount) AS total_loan_amount FROM loans_nick l JOIN loan_type_nick lt ON l.loan_type_id = lt.type_id GROUP BY lt.type_name ORDER BY lt.type_name")
df.show()
df.write.mode("overwrite").saveAsTable("default.loans_by_type_nick")

df.show()
df = spark.sql("SELECT c.customer_name, c.customer_yearly_income, CASE WHEN c.customer_yearly_income > 100000 THEN 'High Earner' WHEN c.customer_yearly_income < 10000 THEN 'Low Earner' ELSE 'Average' END AS income_category, SUM(l.amount) AS total_loan_amount FROM default.customer_info_nick c LEFT JOIN default.loans_nick l ON c.customer_id = l.customer_id GROUP BY c.customer_name, c.customer_yearly_income ORDER BY c.customer_name")
df.write.mode("overwrite").saveAsTable("default.earner_class_nick")

df = spark.sql("WITH progress AS (SELECT l.loan_id, l.customer_id, l.amount, COALESCE(l.amount_paid, 0) AS amount_paid, (l.amount - COALESCE(l.amount_paid, 0)) AS remaining, CASE WHEN l.amount > 0 THEN COALESCE(l.amount_paid,0) / l.amount ELSE NULL END AS pct_paid FROM default.loans_nick l) SELECT p.loan_id, c.customer_id, c.customer_name, p.amount, p.amount_paid, p.remaining, ROUND(p.pct_paid * 100, 2) AS pct_paid_percent FROM progress p JOIN default.customer_info_nick c ON c.customer_id = p.customer_id WHERE p.remaining > 0 AND p.pct_paid >= 0.90 ORDER BY p.pct_paid DESC, p.remaining ASC")
df.show()
df.write.mode("overwrite").saveAsTable("default.almost_paid_nick")

df = spark.sql("SELECT lt.type_name, COUNT(*) AS missed_payment_count FROM default.loan_payments2_nick lp JOIN default.loans_nick l ON lp.loan_id = l.loan_id JOIN default.loan_type_nick lt ON l.loan_type_id = lt.type_id WHERE lp.status IN ('late','missed') GROUP BY lt.type_name ORDER BY missed_payment_count DESC LIMIT 1")
df.show()
df.write.mode("overwrite").saveAsTable("default.most_missed_type_nick")

df = spark.sql("SELECT c.customer_id, c.customer_name, COUNT(*) AS late_payment_count FROM default.loan_payments2_nick lp JOIN default.loans_nick l ON lp.loan_id = l.loan_id JOIN default.customer_info_nick c ON l.customer_id = c.customer_id WHERE lp.status = 'late' GROUP BY c.customer_id, c.customer_name ORDER BY late_payment_count DESC")
df.show()
df.write.mode("overwrite").saveAsTable("default.late_by_customer_nick")

df = spark.sql("WITH progress AS (SELECT l.loan_id, l.customer_id, l.amount, COALESCE(l.amount_paid, 0) AS amount_paid, (l.amount - COALESCE(l.amount_paid, 0)) AS remaining, CASE WHEN l.amount > 0 THEN COALESCE(l.amount_paid,0) / l.amount ELSE NULL END AS pct_paid FROM default.loans_nick l) SELECT p.loan_id, c.customer_id, c.customer_name, p.amount, p.amount_paid, p.remaining, ROUND(p.pct_paid * 100, 2) AS pct_paid_percent FROM progress p JOIN default.customer_info_nick c ON c.customer_id = p.customer_id WHERE p.remaining > 0 AND p.pct_paid < 0.10 ORDER BY p.pct_paid ASC, p.remaining DESC")
df.show()
df.write.mode("overwrite").saveAsTable("default.hardly_paid_nick")



