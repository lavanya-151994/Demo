import datacompy
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *
from datetime import date, datetime
from pyspark.sql.functions import col, when
import pyspark.sql.functions as f
from pyspark.sql.types import StringType


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Bi_Professional") \
        .config("spark.mongodb.input.uri",
                "mongodb+srv://edwprod:edwprod123@edw-prod-pri.rjwwk.mongodb.net/Mycroft") \
        .config("spark.mongodb.output.uri",
                "mongodb+srv://edwprod:edwprod123@edw-prod-pri.rjwwk.mongodb.net/Mycroft") \
        .config("spark.mongodb.input.sampleSize", 50000) \
        .getOrCreate()
        
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)


    start_date = date(2023, 2, 1)

    end_date = date(2023, 2, 10)
    schema = StructType([
        StructField("Adj_Claim_Line_Charges", LongType(), True),
		StructField("Allowed_Amount", LongType(), True),
		StructField("Birth_Date", DateType(), True),
		StructField("COB_Amount", LongType(), True),
		StructField("Claim_Edit_Description", StringType(), True),
		StructField("Claim_Header_Total_Paid", LongType(), True),
		StructField("Claim_ID_QNXT", StringType(), True),
		StructField("Claim_Last_Iteration", StringType(), True),
		StructField("Claim_Line_Charges", LongType(), True),
		StructField("Claim_Line_Date_of_Service_Begin", DateType(), True),
		StructField("Claim_Line_Date_of_Service_End", DateType(), True),
		StructField("Claim_Line_Number", IntegerType(), True),
		StructField("Claim_Line_Status",StringType(), True),
		StructField("Claim_Line_Total_Paid", LongType(), True),
		StructField("Claim_Number", StringType(), True),
		StructField("Claim_Paid_Date", DateType(), True),
		StructField("Claim_Paid_Date_Max", DateType(), True),
		StructField("Claim_Paid_Date_Min", DateType(), True),
		StructField("Claim_Received_Date", DateType(), True),
		StructField("Claim_Status", StringType(), True),
		StructField("Coinsurance_Amount", LongType(), True),
		StructField("Copay_Amount", LongType(), True),
		StructField("DX_CDS_PRNCPL_1", StringType(), True),
		StructField("DX_CDS_PRNCPL_2", StringType(), True),
		StructField("DX_CDS_PRNCPL_3", StringType(), True),
		StructField("DX_CDS_PRNCPL_4", StringType(), True),
        StructField("DX_CDS_PRNCPL_5", StringType(), True),
        StructField("DX_CDS_SECNDRY_1", StringType(), True),		
		StructField("DX_CDS_SECNDRY_2", StringType(), True),
		StructField("DX_CDS_SECNDRY_3", StringType(), True),
		StructField("DX_CDS_SECNDRY_4", StringType(), True),
		StructField("DX_CDS_SECNDRY_5", StringType(), True),
		StructField("DX_CDS_SECNDRY_6", StringType(), True),
		StructField("DX_CDS_SECNDRY_7", StringType(), True),
		StructField("DX_VRSN_CD", StringType(), True),
		StructField("DX_VRSN_DSC", StringType(), True),
		StructField("Diagnosis_Long_Description", StringType(), True),
		StructField("Eligible_Amount", LongType(), True),
		StructField("Family_Alt_ID", StringType(), True),
		StructField("Form_Type", StringType(), True),
		StructField("Funds_Source_Code", StringType(), True),
		StructField("Ind_Alt_ID", StringType(), True),
		StructField("Member_ID", StringType(), True),
		StructField("NDC_Code", StringType(), True),
		StructField("National_Provider_ID_NPI_Billing", StringType(), True),
		StructField("National_Provider_ID_NPI_Performing", StringType(), True),
		StructField("PROC_CDS_PRNCPL_ICD_9CM_10PCS", StringType(), True),
		StructField("PROC_CPT_HCPCS_CD", StringType(), True),
		StructField("PROC_CPT_HCPCS_DSC", StringType(), True),
		StructField("PROC_MODFR_CD_1", StringType(), True),
		StructField("PROC_MODFR_CD_2", StringType(), True),
		StructField("PROC_MODFR_CD_3", StringType(), True),
		StructField("Patient_Age_At_Time_Of_Service", IntegerType(), True),
		StructField("Patient_Control_Number", StringType(), True),
		StructField("Person_City", StringType(), True),
		StructField("Person_First_Name", StringType(), True),
		StructField("Person_Gender", StringType(), True),
		StructField("Person_ID", LongType(), True),
		StructField("Person_Last_Name", StringType(), True),
		StructField("Person_State", StringType(), True),	
		StructField("Person_Zip", LongType(), True),
		StructField("Place_of_Service_Code", StringType(), True),
		StructField("Place_of_Service_Code_Description", StringType(), True),
		StructField("Primary_Speciality_Code", StringType(), True),
		StructField("Procedure_Version_Code", StringType(), True),
		StructField("Provider_City", StringType(), True),
		StructField("Provider_Contract_ID", StringType(), True),
		StructField("Provider_Contract_Term_Description", StringType(), True),
		StructField("Provider_First_Name", StringType(), True),
		StructField("Provider_ID_QNXT_Billing", StringType(), True),
		StructField("Provider_ID_QNXT_Performing", StringType(), True),
		StructField("Provider_ID_QNXT_Referring", StringType(), True),
		StructField("Provider_Last_Name", StringType(), True),
		StructField("Provider_State", StringType(), True),
		StructField("Provider_Type", StringType(), True),
		StructField("Provider_Zip", StringType(), True),
		StructField("QNXT_Provider_ID", StringType(), True),
		StructField("Reporting_Benefit_Plan", StringType(), True),
		StructField("Reporting_Fund_Detail", StringType(), True),
		StructField("Reporting_Plan_Code", StringType(), True),
		StructField("Reporting_Plan_Name", StringType(), True),
		StructField("Reporting_Relationship", StringType(), True),
		StructField("Reporting_Status", StringType(), True),
		StructField("Servicing_Provider_Speciality_1", StringType(), True),
		StructField("Servicing_Provider_Specialty_Description", StringType(), True),
		StructField("Speciality_type", StringType(), True),
		StructField("Standard_Base_Rate", StringType(), True),
		StructField("Subcategory_Of_Expense", StringType(), True),	
		StructField("Tax_ID", StringType(), True),
		StructField("Total_Charges", LongType(), True),
		StructField("Units_Billed", IntegerType(), True),
		StructField("Units_Paid", LongType(), True),
     
    ])
    pipeline = '[{"$match":{"Claim_Paid_Date": { "$gte": new ISODate("2023-02-01"), "$lte": new ISODate("2023-02-10")}}}]'
    mongoDF = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "Mycroft") \
        .option("collection", "BI_Professional_Claims") \
        .option("pipeline",pipeline)\
        .schema(schema).load().drop("_id")
        #.filter(col('Claim_Paid_Date').between(lit(start_date), lit(end_date))).drop("_id")
    # .orderBy(JOIN_COLS,ascending=[True])\
    # .limit(10)

    print("BI_Professional_Claims FROM MONGO:")

    # mongoDF = mongoDF.withColumn("Claim_Line_Number", col("Claim_Line_Number").cast(FloatType()))

    #mongoDF.printSchema()
    mongoDF.show(2)

   
    bigqueryDF = spark.read.format('bigquery')\
        .option("table", "bigquery-prod-321919:SAS_PROD.BI_Professional_Claims")\
		.option('parentProject', 'bigquery-prod-321919')\
        .option('project', 'bigquery-prod-321919')\
        .load().filter((col('Claim_Paid_Date') >= lit(start_date)) & (col('Claim_Paid_Date') <= lit(end_date)))
    bigqueryDF.createOrReplaceTempView("bigqueryDFView")
    print("BI_Professional_Claims FROM bigquery:")
    #bigqueryDF.printSchema()
    bigqueryDF.show(2)
    

    for coln in mongoDF.columns:
        mongoDF = mongoDF.withColumnRenamed(coln, coln.lower().strip())

    for coln in bigqueryDF.columns:
        bigqueryDF = bigqueryDF.withColumnRenamed(coln, coln.lower().strip())

    for coln in mongoDF.columns:
        mongoDF = mongoDF.withColumn(coln, col(coln).cast(StringType()))

    for coln in bigqueryDF.columns:
        bigqueryDF = bigqueryDF.withColumn(coln, col(coln).cast(StringType()))
        
        
    cols = ['speciality_type','provider_id_qnxt_referring','claim_line_number','claim_id_qnxt','provider_contract_term_description','claim_status','subcategory_of_expense','funds_source_code','patient_control_number',
    'person_first_name','person_last_name','primary_speciality_code','proc_cpt_hcpcs_cd','provider_contract_id','provider_id_qnxt_billing','provider_id_qnxt_performing',
    'qnxt_provider_id','provider_last_name','proc_cpt_hcpcs_dsc','claim_line_status','provider_contract_term_description','provider_first_name'
    ]

    for coln in cols:
        bigqueryDF = bigqueryDF.withColumn(coln, trim(col(coln)))
        mongoDF = mongoDF.withColumn(coln, trim(col(coln)))    

    cols = ['birth_date','claim_paid_date','claim_paid_date_max','claim_paid_date_min','claim_received_date','claim_line_date_of_service_end',
    'claim_line_date_of_service_begin']

    for coln in cols:
        bigqueryDF = bigqueryDF.withColumn(coln, to_date(col(coln)))
        mongoDF = mongoDF.withColumn(coln, to_date(col(coln)))
    
    
    bigqueryDF = bigqueryDF.na.fill(value="", subset=cols)
    mongoDF = mongoDF.na.fill(value="", subset=cols)
    
    cols = ['allowed_amount','adj_claim_line_charges','eligible_amount','cob_amount','claim_line_total_paid','claim_line_charges']

    bigqueryDF = bigqueryDF.na.fill(value="0.0", subset=cols)
    mongoDF = mongoDF.na.fill(value="0.0", subset=cols)
    
    
    cols = ['tax_id','provider_type','person_gender','person_last_name','person_first_name','servicing_provider_specialty_description','provider_city','proc_cpt_hcpcs_dsc','person_zip','person_state','person_city','dx_cds_secndry_7','dx_cds_secndry_6','dx_cds_secndry_5','dx_cds_secndry_4','dx_cds_secndry_3','dx_cds_secndry_2','dx_cds_secndry_1','family_alt_id','provider_zip','provider_id_qnxt_referring','provider_state','dx_cds_prncpl_2','dx_cds_prncpl_3','dx_cds_prncpl_3','dx_cds_prncpl_4','dx_cds_prncpl_5','proc_modfr_cd_1','proc_modfr_cd_2','proc_modfr_cd_3']
    bigqueryDF =  bigqueryDF.na.fill(value="",subset=cols)
    mongoDF = mongoDF.na.fill(value="", subset=cols)
    
    
    cols = ['dx_vrsn_cd','procedure_version_code','member_id','units_paid','ind_alt_id','person_id','ind_alt_id']
    bigqueryDF = bigqueryDF.na.fill(value="0", subset=cols)
    mongoDF = mongoDF.na.fill(value="0", subset=cols)
    
    cols = ['patient_control_number','provider_contract_term_description','proc_cpt_hcpcs_cd'] 
    for coln in cols:
        bigqueryDF = bigqueryDF.withColumn(coln,upper(col(coln)))  

    for coln in bigqueryDF.columns:
        bigqueryDF = bigqueryDF.withColumn(coln, col(coln).cast(StringType()))

    for coln in mongoDF.columns:
        mongoDF = mongoDF.withColumn(coln, trim(col(coln)))

    for coln in bigqueryDF.columns:
        bigqueryDF = bigqueryDF.withColumn(coln, trim(col(coln)))        

    #for coln in JOIN_COLS:
    #    mongoDF = mongoDF.withColumn(coln, trim(col(coln)))
    #    bigqueryDF = bigqueryDF.withColumn(coln, trim(col(coln)))

    print("After Column Modification:")
    #mongoDF.printSchema()
    #bigqueryDF.printSchema()

    #bigqueryDF.show(5)
    #mongoDF.show(5)

    #mongoDF.na.fill(value='')
    #bigqueryDF.na.fill(value='')

    # Creating dataframe in pandas
    df1 = mongoDF.toPandas()
    df2 = bigqueryDF.toPandas()

    # df1['claim_id_qnxt'] = df1['claim_id_qnxt'].str.strip()

    print("Dataframe creation done in pandas")

    # Using pandas for datacompy
    compare = datacompy.Compare(
        df1,
        df2,
        join_columns=['claim_line_number','claim_id_qnxt'],
        # You can also specify a list of columns eg ['policyID','statecode']
        abs_tol=0,  # Optional, defaults to 0
        rel_tol=0,  # Optional, defaults to 0
        df1_name='mongo',  # Optional, defaults to 'df1'
        df2_name='bigquery'  # Optional, defaults to 'df2'
    )

    print(compare.report())
