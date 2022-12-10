
from dotenv import load_dotenv
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
import os 
import json 

#load_dotenv for testing only, migrate to using variables in azure functions or not, up to you
load_dotenv()

#Get all the file names of any jobs and only get the name of the jobs
job_files = os.listdir("jobs")
job_list = {}
for job_file in job_files:
    with open(f'jobs/{job_file}', "r") as f:
        job_contents = json.load(f)
        job_list[job_contents['settings']['name']]=job_contents['settings']

#Use Databricks CLI to get all the existing Jobs in the Databricks Workspace
api_client = ApiClient(
  host  = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)
job_api = JobsApi(api_client)
all_jobs = job_api.list_jobs()
jobs = all_jobs['jobs']
existing_jobs = []
for job in jobs:
    existing_jobs.append(job['settings']['name'])

for new_job in job_list:
    if new_job in existing_jobs:
        print(f"Job - {new_job} already Exists")
    else:
        print(f"New Job Detected - {new_job}. Creating new Job")
        job_api.create_job(job_list[new_job])