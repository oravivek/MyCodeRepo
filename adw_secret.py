import os
import oci
import base64
import zipfile
from urllib.parse import urlparse

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def in_dataflow():
    """
    Determine if we are running in OCI Data Flow by checking the environment.
    """
    if os.environ.get("HOME") == "/home/dataflow":
       return True
    return False

def get_authenticated_client(token_path, client, file_location=None, profile_name=None):
    """
    Get an an authenticated OCI client.
    Example: get_authenticated_client(token_path, oci.object_storage.ObjectStorageClient)
    """
    import oci

    if not in_dataflow():
       # We are running locally, use our API Key.
       if file_location is None:
             file_location = oci.config.DEFAULT_LOCATION
       if profile_name is None:
             profile_name = oci.config.DEFAULT_PROFILE
       config = oci.config.from_file(file_location=file_location, profile_name=profile_name)
       authenticated_client = client(config)
    else:
       # We are running in Data Flow, use our Delegation Token.
       with open(token_path) as fd:
             delegation_token = fd.read()
       signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
             delegation_token=delegation_token
       )
       authenticated_client = client(config={}, signer=signer)
    return authenticated_client

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("Sample Data Flow App") \
    .getOrCreate()
    
print("Reading data from object storage !")

src_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .load(
        "oci://Public_Bucket@orasenatdpltintegration01/201306-citibike-tripdata.csv" # Datafile location in OCI Object Storage
    )
    .cache()
)  # cache the dataset to increase computing speed

src_df.show(5)
# Get an Object Store client
#token_key = "spark.hadoop.fs.oci.client.auth.delegationTokenPath"
#token_path = spark.sparkContext.getConf().get(token_key)
#configurations = spark.sparkContext.getConf().getAll()
#for item in configurations: print(item)
    
#    file_location = oci.config.DEFAULT_LOCATION
#    profile_name = oci.config.DEFAULT_PROFILE
#    config = oci.config.from_file(file_location=file_location, profile_name=profile_name)
#    authenticated_client = client(config)
#    return authenticated_client

import base64

token_key = "spark.hadoop.fs.oci.client.auth.delegationTokenPath"
token_path = spark.sparkContext.getConf().get(token_key)

secrets_client = get_authenticated_client(token_path, oci.secrets.SecretsClient)

password_ocid = "ocid1.vaultsecret.oc1.iad.amaaaaaaytsgwayawkv2f4ruhsxdbcqi2khi5wzgceeo3yj4iwbuhbxi6iiq"

response = secrets_client.get_secret_bundle(password_ocid)

base64_secret_content = response.data.secret_bundle_content.content
base64_secret_bytes = base64_secret_content.encode("ascii")
base64_message_bytes = base64.b64decode(base64_secret_bytes)
secret_content = base64_message_bytes.decode("ascii")
print(secret_content)

src_df.write \
    .format("oracle") \
    .option("adbId","ocid1.autonomousdatabase.oc1.iad.anuwcljtytsgwaya2qd7ucjplxk5k4fene6lfmwjocm2e2gdzmxz65eafqya") \
    .option("connectionId","ladybirddb_high") \
    .option("dbtable", "ADMIN.TARGETTBL") \
    .option("user", "ADMIN") \
    .option("password", secret_content) \
    .mode("Overwrite") \
    .save()

print("\nWriting data to autonomous database.")