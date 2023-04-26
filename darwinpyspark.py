import json
import requests
import urllib.request
import urllib.parse
import zipfile
from io import BytesIO

class DarwinPyspark:
    def __init__(self, API_KEY, team_slug, dataset_slug):
        """
        Method to initialise Darwin Pyspark
        """
        self.headers = {
                "accept": "application/json",
                "Authorization": f"ApiKey {API_KEY}",
                "User-Agent": "v7-labs"
            }
        self.team_slug = team_slug.lower().strip().replace(" ",  '-')
        self.dataset_slug = dataset_slug.lower().strip().replace(" ",  '-')
        

    def data_registration(self, item_name):
        
        """
        Method to register items and slots
        """

        url = f"https://darwin.v7labs.com/api/v2/teams/{self.team_slug}/items/register_upload"

        payload = {
            "items": [
                {
                    "slots": [
                        {
                            "tags": [],
                            "file_name": item_name,
                            "slot_name": "0"
                        }
                    ],
                    "name": item_name,
                    "layout": None,
                    "path": "",
                    "tags": []
                }
            ],
            "dataset_slug": self.dataset_slug
        }

        response = requests.post(url, headers=self.headers,json=payload)

        json_response = json.loads(response.text)

        if not json_response["blocked_items"]:
            upload_id = json_response["items"][0]["slots"][0]["upload_id"]
        else:
            upload_id = None

        return upload_id
        

    def sign_upload(self, upload_id):
        """
        Method to sign upload for an item
        """

        url = f"https://darwin.v7labs.com/api/v2/teams/{self.team_slug}/items/uploads/{upload_id}/sign"

        response = requests.get(url, headers=self.headers)
        upload_url = json.loads(response.text)["upload_url"]

        return upload_url


    def upload_binary(self, item_path, upload_url):
        """
        Method to upload item data to the V7 platform
        """

        encoded_url = urllib.parse.quote(item_path, safe=':/')
        response = urllib.request.urlopen(encoded_url)
        data = response.read()

        response = requests.put(url=upload_url,
                            data=data,
                            headers={'Content-Type': 'application/octet-stream'})
                            
        if response.ok:
            response_message = f"{item_path} image data has been uploaded to V7"
        else:
            response_message = f"Issue uploading {item_path} data to V7"

        return response_message


    def confirm(self, upload_id, item_name):
        """
        Method to confirm an upload for a particular upload_id
        """
        url = f"https://darwin.v7labs.com/api/v2/teams/{self.team_slug}/items/uploads/{upload_id}/confirm"
        response = requests.post(url, headers=self.headers)

        return response


    def upload_item(self, item_name, item_path):
        """
        Method to call all upload methods and upload a specific item to V7
        """
        upload_id = self.data_registration(item_name)
        if upload_id != None:
            upload_url = self.sign_upload(upload_id)
            self.upload_binary(item_path, upload_url)
            self.confirm(upload_id, item_name)
        else:
            pass


    def upload_items(self, df):
        """
        Method to upload a pyspark dataframes data to V7
        """
        df.select("file_name", "object_urls").foreach(lambda row: self.upload_item(row[0], row[1])) 

        
    def flatten_dict(self, d, parent_key=''):
        """
        Method to flatten the darwin export json to make ready for write to pyspark table
        """
        flat_dict = {}
        for k, v in d.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if k == 'annotations':
                # Special case for annotations key
                for i, annotation in enumerate(v):
                    for ak, av in annotation.items():
                        flat_dict[f"{new_key}_{i}_{ak}"] = av
            elif isinstance(v, dict):
                flat_dict.update(self.flatten_dict(v, new_key))
            else:
                flat_dict[new_key] = v
        return flat_dict

    
    def get_export_url(self, export_name):  
        """
        Method to get the url for the export to be downloaded
        """
        url = f"https://darwin.v7labs.com/api/v2/teams/{self.team_slug}/datasets/{self.dataset_slug}/exports"
        response = requests.get(url, headers=self.headers)
        if response.ok:
            # The response content will be the exported dataset in JSON format
            export_json = response.content
        else:
            # Handle the error response
            print(f"Error: {response.status_code} - {response.reason}")

        # get the export zip url
        for export_json in json.loads(export_json.decode()):
            if export_json["name"] == export_name:
                download_url = export_json["download_url"]
            else:
                download_url = None
        return download_url

    
    def download_export_zip(self, download_url):
        """
        From the export url, method to download the relevant darwin json export
        """
        # download the zip file from the URL
        response = urllib.request.urlopen(download_url)
        return zipfile.ZipFile(BytesIO(response.read()))

    
    # def extract_export(self, zipfile, sc, spark):
    #     """
    #     Method to write the darwin json results to a pyspark dataframe
    #     """

    #     # Set Databricks user agent tag
    #     spark.conf.set("spark.databricks.agent.id", "v7-labs")

    #     # extract the JSON files and read them into a DataFrame
    #     json_files = []
    #     for filename in zipfile.namelist():
    #         if filename.endswith('.json'):
    #             data = zipfile.read(filename)
    #             json_files.append(self.flatten_dict(json.loads(data)))
    #     return spark.read.json(sc.parallelize(json_files))

    def extract_export(self, zipfile, sc, spark):
        """
        Method to write the darwin json results to a pyspark dataframe
        """

        # Set Databricks user agent tag
        spark.conf.set("spark.databricks.agent.id", "v7-labs")

        # extract the JSON files and read them into a DataFrame
        json_files = []
        for filename in zipfile.namelist():
            if filename.endswith('.json'):
                data = zipfile.read(filename)
                json_files.append(data.decode('utf-8'))

        # Define the schema for the JSON data
        schema = "struct<"
        for key in json.loads(json_files[0]).keys():
            schema += f"`{key}` string,"
        schema = schema[:-1] + ">"

        # Create a DataFrame from the JSON data and parse the JSON strings
        df = spark.createDataFrame(json_files, "string")
        df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

        return df


    def download_export(self, export_name):
        """
        Calls all download methods to get and write an export to a pyspark dataframe
        """
        export_url = self.get_export_url(export_name)
        if export_url != None:
            # create a SparkSession object
            spark = SparkSession.builder.appName("v7-labs").getOrCreate()

            # create a SparkContext object
            sc = spark.sparkContext

            export_df = self.extract_export(self.download_export_zip(export_url), sc, spark)
        else:
            export_df = "No Valid Export With That Name"

        return export_df