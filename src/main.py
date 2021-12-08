import os
import sys
from pathlib import Path

#Setting Environmnet variables
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-11.0.13'
os.environ['HADOOP_HOME'] = r'C:\Spark\spark-3.0.3-bin-hadoop2.7'

#Setting Relative paths
import sys
import os
sys.path.append(os.path.abspath('../src'))


#Imports required
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

def rename_col(df,old_n,new_n):
    '''
        Receives a dataframe so as to rename a particular
        field with a given new name
    '''
    return df.withColumnRenamed(old_n,new_n)

def filter_countries(df,include_countries):
    '''
        Filters out rows in the DF based on the country field
        from a given list of countries
    '''
    return df.filter(df.country.isin(include_countries))



#Main method
if __name__ == '__main__':

    #Getting the working directory of the script
    base_path = Path(__file__).parent.parent
    print(base_path)

    #setting the appropriate output directory to save the client file
    relpath = "\client_data\\"
    abs_output_dir = str(base_path) + relpath
    print(abs_output_dir)


    #Logging variables
    log_file = "main_log.txt"
    logging.basicConfig(filename=log_file,level=logging.INFO,format='%(asctime)s - %(levelname)s :  %(message)s',datefmt='%Y/%m/%d %H:%M:%S')

    #Stop the script if the input parameters are not equal to 3
    if(len(sys.argv) != 4):
        logging.error("Error!.. Input parameters not equal to three")
        sys.exit()

    #Declaring user input variables
    ds1_f = sys.argv[1]
    ds2_f = sys.argv[2]
    country_list_str = sys.argv[3]
    country_list = country_list_str.split(",")
    print(country_list)
    
    #Assigning an output filename
    output_file = "dataset_client_" + datetime.now().strftime("%Y%m%d%H%M") + ".csv"

    logging.info("----------Starting Process----------")

    #Initialize Spark Session
    spark = SparkSession.builder.getOrCreate()

    try:
        #Reading the first dataset and removing any personal identifiable fields
        logging.info("Reading the first dataset found in {}".format(ds1_f))
        df_customer = spark.read.format("csv").option("header",True).load(ds1_f).drop('first_name').drop('last_name')
        print("Number of customers", df_customer.count())

        #Reading the second dataset and removing any personal identifiable fields
        logging.info("Reading the second dataset found in {}".format(ds2_f))
        df_customer_filtered = filter_countries(df_customer,country_list)
        print("Number of customers after filtering", df_customer_filtered.count())

        #Filtering the rows which are only found for the given country list by the user
        logging.info("Filtering for the following countries {}".format(country_list))
        df_financial = spark.read.format("csv").option("header",True).load(ds2_f).drop('cc_n')
        print("Number of financial statements ", df_financial.count())

        #Renaming the id field before joining so as it is unique
        df_financial = rename_col(df_financial,"id","id_fin")
        df_financial.show()
        
        #Joining the two datasets together based on the id field
        df_joined = df_customer_filtered.join(df_financial, df_customer_filtered.id == df_financial.id_fin, 'inner').drop(F.col('id_fin'))

        #renaming the fields of the final dataset
        logging.info("Renaming the output fields")
        df_joined = rename_col(df_joined,"id","client_identifier")
        df_joined = rename_col(df_joined,"btc_a","bitcoin_address")
        df_joined = rename_col(df_joined,"cc_t","credit_card_type")

        df_joined.show()

        #Saving the Final Dataframe as a csv file
        #df_joined.write.csv(output_file)
        logging.info("Saving client dataset named as {}".format(output_file))
        df_joined.toPandas().to_csv(abs_output_dir + output_file, index = False)

        logging.info("Process finished..")
        logging.info("------------------------------------------------------------------------")

    #If there is an error save it to the log file
    except Exception as e:
        logging.error("Error in the process!: {}".format(str(e)))