import os
from typing import TypeVar, Callable
import ichor
from werkzeug.datastructures import Headers
from ichor.apis import BatchJobsApi
from ichor.apis import PatientsApi
from ichor.apis import DataInstancesApi
T = TypeVar('T')
_ichor_api_client = None
_ichor_api_cache = {}


def load_ichor_configuration():
    global _ichor_api_client
    # configuration = ichor.Configuration(host=os.environ['ICHOR_API_ENDPOINT'],
    #                                     api_key={'ApiKeyAuth': os.environ['ICHOR_API_KEY']})
    configuration = ichor.Configuration(host="http://192.168.43.68:1234",
                                        api_key={'ApiKeyAuth': "2YahbXFO11mkBBzfN3R0fQ=="})

    _ichor_api_client = ichor.ApiClient(configuration)
    _ichor_api_client.__enter__()


def get_ichor_api(api: Callable[[], T]) -> T:
    if api not in _ichor_api_cache:
        _ichor_api_cache[api] = api(_ichor_api_client)
    return _ichor_api_cache[api]


def get_job_results(batch_job_id):
    return get_ichor_api(BatchJobsApi).jobs_batch_job_id_results_get(batch_job_id)


# TODO: check it
def download_instance(data_instance_id, target_path='.'):
    import boto3
    s3 = boto3.resource("s3")
    record_files = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_files_get(data_instance_id)
    for record_file in record_files:
        key = record_file.storage_key
        bucket = record_file.storage_bucket
        s3.download_file(
            Bucket=bucket, Key=key, Filename=target_path + "/" + os.path.basename(record_file.original_file_path)
        )


# TODO:
def register_file_to_job(file_id, tag):
    pass


# TODO:
def register_file_to_patient(file_id, patient_id, tag):
    get_ichor_api(PatientsApi).patients_patient_id_put(patient_id=patient_id, patient={"add_files": file_id})


# TODO:
def register_file_to_data_instance(file_id, data_instance_id, tag):
    pass


def get_job_files(batch_job_id):
    return get_ichor_api(BatchJobsApi).jobs_batch_job_id_files_get(batch_job_id)


def get_job_result(batch_job_id, iterable_index):
    return get_ichor_api(BatchJobsApi).jobs_batch_job_id_results_by_iterable_iterable_index_get(iterable_index=iterable_index, batch_job_id=batch_job_id)


def update_job_result(batch_job_id, iterable_index, update_data):
    return get_ichor_api(BatchJobsApi).jobs_batch_job_id_results_by_iterable_iterable_index_put(batch_job_id=batch_job_id, iterable_index=iterable_index, job_result=update_data)


def add_job_result(batch_job_id, iterable_index, job_result):
    return get_ichor_api(BatchJobsApi).jobs_batch_job_id_results_by_iterable_iterable_index_post(batch_job_id=batch_job_id, iterable_index=iterable_index, job_result=job_result)


if __name__ == '__main__':
    counter = 1

    def print_separate():
        global counter
        print("\n\n---------------------------------------------------\n{}".format(counter))
        counter += 1
    load_ichor_configuration()
    update_data = {
        "add_files": [1, 2, 3, 4, 5]
    }

    # 1
    print_separate()
    patient = get_ichor_api(PatientsApi).patients_patient_id_get(1)
    print("\n***** add files to patient *****")
    print("\n***** patient ID: 1 *****")
    print(patient)
    updated_patient = get_ichor_api(PatientsApi).patients_patient_id_put(1, patient=update_data)
    print("\n***** patient ID 1 after update *****")
    print(updated_patient)

    # 2
    print_separate()
    data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_get(1)
    print("\n***** add files to data instance *****")
    print("\n***** Data Instance ID: 1 *****")
    print(data_instance)
    updated_data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_put(1, data_instance=update_data)
    print("\n***** Data Instance ID 1 after update *****")
    print(updated_data_instance)

    # 3
    print_separate()
    print("\n***** get Job results of ID 1 *****")
    job_results = get_job_results(1)
    print(job_results)

    # 4
    print_separate()
    print("\n***** get Job files of ID 1 *****")
    job_files = get_job_files(1)
    print(job_files)

    # 5
    print_separate()
    iterable_index = 4
    print("\n***** update job result *****")
    print("\n***** before update *****")
    updated_job_result = get_job_result(iterable_index=iterable_index, batch_job_id=1)
    print(updated_job_result)
    print(f"\n***** update Job result of job ID 1 , iterable {iterable_index} *****")
    update_data = {
        "batch_job_id": 1,
        "iterable_value": 123,
        "iterable_index": iterable_index,
        "files": {"tag": 1},
        "console_output": "string",
        "exit_code": 0,
        "free_form_data": {"free": "form", "data": "data"},
    }
    update_job_result(batch_job_id=1, iterable_index=iterable_index, update_data=update_data)
    print("\n***** after update *****")
    updated_job_result = get_job_result(iterable_index=iterable_index, batch_job_id=1)
    print(updated_job_result)

    # 6
    print_separate()
    iterable_index = 39
    print(f"\n***** post Job results of job ID 1 , iterable {iterable_index} *****")
    new_job_result = add_job_result(1, iterable_index, update_data)
    print(new_job_result)

# TODO: edge cases:
#  add_files in put dataInstance and Patient - add file that exist
#  put, get job result of job and iterable - when iterable doesnt exist.
#  post job result of job and iterable - when iterable doesnt exist in iterables field or file id that doesnt exist.
#  register file to job. - alex
#  include all fields in jobResultSchema - post and put - you can send any field now
#  put job result - replace it totally or just fields inside. i did replace totally.
#  insert in job result wrong iterable value and index

# TODO: add to backend:
#  delete job result?
#  output of methods
#  remove key and value from fileSchema



