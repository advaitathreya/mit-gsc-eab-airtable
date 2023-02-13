import requests
import json
import pandas as pd
import tqdm


def airtable_download(table, params_dict={}, api_key=None, base_id=None, record_id=None):
    """Makes a request to Airtable for all records from a single table.
        Returns data in dictionary format.

    Keyword Arguments:
    • table: set to table name
        ◦ see: https://support.airtable.com/hc/en-us/articles/360021333094#table
    • params_dict: desired parameters in dictionary format {parameter : value}
        ◦ example: {"maxRecords" : 20, "view" : "Grid view"}
        ◦ see "List Records" in API Documentation (airtable.com/api)
    • api_key: retrievable at https://airtable.com/account
        ◦ looks like "key●●●●●●●●●●●●●●"
    • base_id: retrievable at https://airtable.com/api for specific base
        ◦ looks like "app●●●●●●●●●●●●●●"
    • record_id: optional for single record lookups
        ◦ looks like "rec●●●●●●●●●●●●●●"
        """

    # Authorization Credentials
    if api_key is None:
        print("Enter Airtable API key. \n  *Find under Airtable Account Overview: https://airtable.com/account")
        api_key = input()
    headers = {"Authorization": "Bearer {}".format(api_key)}
    validate_airtable_kwargs(api_key, "API key", "pat", char_length=82)

    # Locate Base
    if base_id is None:
        print("Enter Airtable Base ID. \n  *Find under Airtable API Documentation: https://airtable.com/api for specific base")
        base_id = input()
    url = 'https://api.airtable.com/v0/{}/'.format(base_id)
    path = url + table
    validate_airtable_kwargs(base_id, "Base ID", "app")

    # Validate Record ID
    if record_id is not None:
        validate_airtable_kwargs(record_id, "Record ID", "rec")

    # Format parameters for request
    constant_params = ()
    for parameter in params_dict:
        constant_params += ((parameter, params_dict[parameter]),)
    params = constant_params

    # Start with blank list of records
    airtable_records = []

    # Retrieve multiple records
    if record_id is None:
        run = True
        while run is True:
            response = requests.get(path, params=params, headers=headers)
            airtable_response = response.json()

            try:
                airtable_records += (airtable_response['records'])
            except:
                if 'error' in airtable_response:
                    identify_errors(airtable_response)
                    return airtable_response

            if 'offset' in airtable_response:
                run = True
                params = (('offset', airtable_response['offset']),) + constant_params
            else:
                run = False

    # Retrieve single record
    if record_id is not None:
        if params_dict != {}:
            print("⚠️ Caution: parameters are redundant for single record lookups. Consider removing `params_dict` argument.")
        path = "{}/{}".format(path, record_id)
        response = requests.get(path, headers=headers)
        airtable_response = response.json()

        if 'error' in airtable_response:
            identify_errors(airtable_response)
            return airtable_response

        airtable_records = [airtable_response]

    return airtable_records


def convert_to_dataframe(airtable_records):
    """Converts dictionary output from airtable_download() into a Pandas dataframe."""
    airtable_rows = []
    airtable_index = []
    for record in airtable_records:
        airtable_rows.append(record['fields'])
        airtable_index.append(record['id'])
    airtable_dataframe = pd.DataFrame(airtable_rows, index=airtable_index)
    return airtable_dataframe


def create_field_matching_dict(airtable_records, value_field, key_field = None, swap_pairs = False):
    """Uses airtable_download() output to create a dictionary that matches field values from
    the same record together. Useful for keeping track of relational data.
    
    If second_field is `None`, then the dictionary pairs will be {<record id>:value_field}.
    Otherwise, the dictionary pairx will be {key_field:value_field}.
    If swap_pairs is True, then dictionary pairs will be {value_field:<record id>(or key_field)}.
    """
    airtable_dict = {}
    for airtable_record in airtable_records:
        if key_field == None:
            key = airtable_record['id']
        else:
            key = airtable_record['fields'].get(key_field)
        value = airtable_record['fields'].get(value_field)
        if swap_pairs:
            airtable_dict.update({key : value})
        else:
            airtable_dict.update({value : key})
    return airtable_dict


def airtable_upload(table, upload_data, typecast = False, api_key = None, base_id = None, record_id = None):
    """Sends dictionary data to Airtable to add or update a record in a given table. 
        Returns new or updated record in dictionary format.
    
    Keyword arguments:
    • table: set to table name
        ◦ see: https://support.airtable.com/hc/en-us/articles/360021333094#table
    • upload_data: a dictionary of fields and corresponding values to upload in format {field : value}
        ◦ example: {"Fruit" : "Apple", "Quantity" : 20}
    • typecast: if set to true, Airtable will attempt "best-effort automatic data conversion from string values"
        • see: "Create Records" or "Update Records" in API Documentation, available at https://airtable.com/api for specific base
    • api_key: retrievable at https://airtable.com/account
        ◦ looks like "key●●●●●●●●●●●●●●"
    • base_id: retrievable at https://airtable.com/api for specific base
        ◦ looks like "app●●●●●●●●●●●●●●"
    • record_id: when included function will update specified record will be rather than creating a new record
        ◦ looks like "rec●●●●●●●●●●●●●●"
        """
    
    # Authorization Credentials
    if api_key == None:
        print("Enter Airtable API key. \n  *Find under Airtable Account Overview: https://airtable.com/account")
        api_key = input()
    headers = {"Authorization" : "Bearer {}".format(api_key),
              'Content-Type': 'application/json'}
    validate_airtable_kwargs(api_key, "API key", "pat", char_length=82)

    # Locate Base
    if base_id == None:
        print("Enter Airtable Base ID. \n  *Find under Airtable API Documentation: https://airtable.com/api for specific base]")
        base_id = input()
    url = 'https://api.airtable.com/v0/{}/'.format(base_id)
    path = url + table
    validate_airtable_kwargs(base_id, "Base ID", "app")
    
    # Validate Record ID
    if record_id != None:
        validate_airtable_kwargs(record_id, "Record ID", "rec")
    
    # Validate upload_data
    if type(upload_data) != dict:
        print("❌ Error: `upload_data` is not a dictonary.")
        return

    # Create New Record
    if record_id == None:
        upload_dict = {"records": [{"fields" : upload_data}], "typecast" : typecast}
        upload_json = json.dumps(upload_dict)
        response = requests.post(path, data=upload_json, headers=headers)
        airtable_response = response.json()

    # Update Record
    if record_id != None:
        path = "{}/{}".format(path, record_id)
        upload_dict = {"fields" : upload_data, "typecast" : True}
        upload_json = json.dumps(upload_dict)
        response = requests.patch(path, data=upload_json, headers=headers)
        airtable_response = response.json()
    
    # Identify Errors
    if 'error' in airtable_response:
        identify_errors(airtable_response, record_id)
        
    return airtable_response


def upload_pandas_dataframe(pandas_dataframe, table, api_key, base_id):
    """Uploads a Pandas dataframe to Airtable. If Pandas index values are Airtable Record IDs, will attempt to update 
        record. Otherwise, will create new records."""
    pandas_dicts = pandas_dataframe.to_dict(orient="index")
    for pandas_dict in tqdm.tqdm(pandas_dicts):
        record_id = pandas_dict
        if validate_airtable_kwargs(str(record_id), "Record ID", "rec", print_messages=False) is False:
            record_id = None
        upload_data = pandas_dicts[pandas_dict]
        airtable_upload(table, upload_data, api_key=api_key, base_id=base_id, record_id=record_id)
    return


# Troubleshooting Functions
def validate_airtable_kwargs(kwarg, kwarg_name, prefix, char_length=17, print_messages=True):
    """Designed for use with airtable_download() and airtable_upload() functions.
        Checks `api_key`, `base_id` and `record_id` arguments to see if they conform to the expected Airtable API format.
        """
    valid_status = True
    if len(kwarg) != char_length:
        if print_messages is True:
            print("⚠️ Caution: {} not standard length. Make sure API key is {} characters long.".format(kwarg_name, char_length))
        valid_status = False
    if kwarg.startswith(prefix) is False:
        if print_messages is True:
            print("⚠️ Caution: {} doesn't start with `{}`.".format(kwarg_name, prefix))
        valid_status = False
    return valid_status


def identify_errors(airtable_response, record_id):
    """Designed for use with airtable_download() and airtable_upload() functions.
        Prints error responses from the Airtable API in an easy-to-read format.
        """
    if 'error' in airtable_response:
        try:
            print('❌ Record {}: {} error: "{}"'.format(record_id, airtable_response['error']['type'], airtable_response['error']['message']))
        except:
            print("❌ Record {}: Error: {}".format(record_id, airtable_response['error']))
    return
