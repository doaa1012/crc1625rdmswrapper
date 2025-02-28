import requests
import re
import json
import pandas as pd
import os
# The simple REST API client for MatInf VRO API
class MatInfWebApiClient:
    def __init__(self, service_url, api_key):
        self.service_url = service_url + "/vroapi/v1/"
        self.api_key = api_key
        self.file_name=""
        self.dataframe=None

    def getFilename_fromCd(self, cd):
        if not cd:
            return None
        fname = re.findall('filename=(.+)(?:;.+)', cd)
        if len(fname) == 0:
            return None
        self.file_name=fname[0]
        return fname[0]

    def get_headers(self):
        return { 'VroApi': self.api_key }

    def execute(self, sql):
        headers = self.get_headers()
        data = { 'sql': sql }
        try:
            response = requests.post(self.service_url+"execute", headers=headers, data=data)
            response.raise_for_status()  # Raise exception for non-2xx status codes
            js = response.json()
            self.dataframe = pd.DataFrame.from_dict(js)
            self.dataframe
            return js
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
    def get_filtered_objects(self, typename_list, sample_typename, start_date, end_date):
        """
        Generates and executes a SQL query with dynamic typename filters and date range.

        :param typename_list: List of associated typenames (e.g., ['EDX CSV', 'Composition'])
        :param sample_typename: The first typename for the main object (e.g., 'Sample')
        :param start_date: Start date as a string (e.g., '2024-01-01')
        :param end_date: End date as a string (e.g., '2024-12-31')
        :return: Tuple containing DataFrame, grouped data as dictionary, object link mapping, and list of object IDs
        """
        # Convert list of typenames to SQL string format
        typename_str = ", ".join(f"'{typename}'" for typename in typename_list)

        # Define the query
        query = f"""
        SELECT 
            o.objectid AS main_objectid, 
            o.objectname,
            t.typename AS sample_typename,
            o._created AS created_date,
            o._updated AS updated_date,
            o.objectfilepath AS main_objectfilepath,
            linked_oi.objectid AS linked_objectid,
            linked_oi.objectfilepath AS linked_objectfilepath,
            ti.typename AS associated_typename
        FROM vroObjectinfo o
        JOIN vroTypeinfo t ON o.typeid = t.typeid
        JOIN vroObjectlinkobject olo ON o.objectid = olo.objectid
        JOIN vroObjectinfo linked_oi ON olo.linkedobjectid = linked_oi.objectid
        JOIN vroTypeinfo ti ON linked_oi.typeid = ti.typeid
        WHERE t.typename = '{sample_typename}'
        AND ti.typename IN ({typename_str})
        AND o._created BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY o.objectid;
        """
        
        # Execute the query
        result = self.execute(query)
        df = pd.DataFrame(result)

        # Create object link mapping
        object_link_mapping = df.groupby("main_objectid")["linked_objectid"].apply(list).to_dict()
        
        # Extract list of main_objectid values
        object_ids = df["main_objectid"].unique().tolist()
        
        return df, object_link_mapping, object_ids
    
    def filter_samples_by_elements(self, object_ids, element_criteria):
        """
        Filters samples based on element names and percentage range.
        """
        if not object_ids:
            print("No objects found in the previous step.")
            return pd.DataFrame()

        # Remove duplicates
        unique_object_ids = list(set(object_ids))


        if not unique_object_ids:
            print("Error: No valid object IDs found. Skipping query.")
            return pd.DataFrame()

        # Convert list to comma-separated string for SQL query
        object_ids_str = ", ".join(map(str, unique_object_ids))
  
        query = f"""
        SELECT 
            s.sampleid, 
            s.elemnumber, 
            s.elements
        FROM vroSample s
        WHERE s.sampleid IN ({object_ids_str});
        """
        # Execute query
        result = self.execute(query)

        # Check if API returned None or empty data
        if not result:
            print("Error: API returned None or empty response.")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(result)
        df["elements"] = df["elements"].astype(str)

        # Filter samples based on required elements and optional percentage range
        required_elements = set(element_criteria.keys())
        
        filtered_df = df[df["elements"].apply(lambda x: bool(required_elements & set(x.strip("-").split("-"))))]
        sample_ids = filtered_df["sampleid"].tolist()
        return filtered_df, sample_ids


    def filter_samples_by_elements_and_composition(self, sample_ids, object_link_mapping, element_criteria):
        """
        Filters samples based on element names and percentage range, including linked Composition objects.
        Then, filters object_link_mapping based on the final filtered DataFrame.
        """
        print("Starting element composition filtering...")

        # Step 1: Validate Inputs
        if not sample_ids:
            print("No objects found in the previous step.")
            return pd.DataFrame(), {}

        unique_sample_ids = set(sample_ids)

        # Filter mapping to keep only relevant sample IDs
        filtered_mapping = {k: v for k, v in object_link_mapping.items() if k in unique_sample_ids}

        if not filtered_mapping:
            print("No matching samples found in the mapping. Skipping query.")
            return pd.DataFrame(), {}

        #print("Filtered Mapping (Before Filtering):", filtered_mapping)  

        # Collect all linked object IDs (flatten the lists)
        linked_object_ids = set(val for sublist in filtered_mapping.values() for val in sublist)

        if not linked_object_ids:
            print("No linked object IDs found. Skipping query.")
            return pd.DataFrame(), {}

        #print("Linked Object IDs:", linked_object_ids)  

        linked_object_ids_str = ", ".join(map(str, linked_object_ids))  # Convert to SQL-friendly format

        # Step 2: Query Composition table for element percentages
        query_composition = f"""
        SELECT sampleid, elementname, valuepercent
        FROM vroComposition
        WHERE sampleid IN ({linked_object_ids_str});
        """

        composition_result = client.execute(query_composition)

        if not composition_result:
            print("Error: No composition data found.")
            return pd.DataFrame(), {}

        df = pd.DataFrame(composition_result)

        if df.empty:
            print("Warning: No matching composition data found.")
            return pd.DataFrame(), {}


        # Step 3: Ensure required columns exist
        if "elementname" not in df.columns or "valuepercent" not in df.columns:
            print("Error: Required columns not found in API response.")
            return pd.DataFrame(), {}

        # Debugging: Ensure all element names match case and formatting
        df["elementname"] = df["elementname"].str.strip().str.lower()
        element_criteria = {k.lower(): v for k, v in element_criteria.items()}
        filter_condition = None

        for element, (min_percentage, max_percentage) in element_criteria.items():
            condition = (
                (df["elementname"] == element) &
                ((df["valuepercent"] >= min_percentage) & (df["valuepercent"] <= max_percentage))
            )

            filter_condition = condition if filter_condition is None else filter_condition | condition

        if filter_condition is not None:
            df = df[filter_condition]

        matched_sample_ids = set(df["sampleid"].unique())

        final_filtered_mapping = {
            k: [obj_id for obj_id in v if obj_id in matched_sample_ids]
            for k, v in filtered_mapping.items()
        }

        # Remove empty lists from final mapping
        final_filtered_mapping = {k: v for k, v in final_filtered_mapping.items() if v}

        return df, final_filtered_mapping


    def download(self, id, file_name=None):
        headers = self.get_headers()
        data = { 'id': id }
        try:
            response = requests.get(self.service_url+"download", params=data, headers=headers)
            response.raise_for_status()  # Raise exception for non-2xx status codes

            self.file_name=file_name
            if not file_name:
                file_name = self.getFilename_fromCd(response.headers.get('content-disposition'))
                #print('extracted file_name: ' + self.file_name)
            open(file_name, 'wb').write(response.content)
            return response

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None


    def process_data(self, typename_list=None, sample_typename=None, start_date=None, end_date=None, 
                     element_criteria=None, download_folder="downloaded_files", output_filename="final.csv", save_location="."):
        """
        Process data based on provided parameters:
        - If typename_list, sample_typename, start_date, and end_date are provided, call get_filtered_objects
        - If element_criteria is provided, call filter_samples_by_elements
        - If element_criteria includes percentages, call filter_samples_by_elements_and_composition
        - Ensures that each filtering step refines the dataset without overriding previous results.
        - Downloads associated files for linked objects while keeping their original filenames and formats.
        - Saves results to a user-defined file in a user-defined location.

        :param typename_list: List of associated typenames.
        :param sample_typename: Main object typename.
        :param start_date: Filtering start date.
        :param end_date: Filtering end date.
        :param element_criteria: Dictionary with element filtering criteria.
        :param download_folder: Folder to save downloaded files (default: "downloaded_files").
        :param output_filename: Name of the output CSV file (default: "final.csv").
        :param save_location: Directory to save the output file (default: current directory).
        :return: Processed DataFrame.
        """

        # Ensure the save location exists
        os.makedirs(save_location, exist_ok=True)

        # Adjust download folder path to be inside save_location
        download_folder = os.path.join(save_location, download_folder)
        os.makedirs(download_folder, exist_ok=True)  # Ensure download folder exists

        # Step 1: Retrieve filtered objects
        df_filtered, object_link_mapping, object_ids = self.get_filtered_objects(typename_list, sample_typename, start_date, end_date)

        # Step 2: Apply element criteria filtering if provided
        if element_criteria:
            df_samples, sample_ids = self.filter_samples_by_elements(object_ids, element_criteria)
            df_filtered = df_filtered[df_filtered["main_objectid"].isin(sample_ids)]

        # Step 3: Apply element criteria with percentages if provided
        if element_criteria and any("percentage" in key for key in element_criteria):
            df_final, final_filtered_mapping = self.filter_samples_by_elements_and_composition(sample_ids, object_link_mapping, element_criteria)
            df_filtered = df_filtered[df_filtered["main_objectid"].isin(final_filtered_mapping.keys())]
        else:
            final_filtered_mapping = object_link_mapping

        # Create a mapping DataFrame
        df_filtered_mapping = pd.DataFrame(
            [(k, v) for k, values in final_filtered_mapping.items() for v in values],
            columns=["SampleID", "LinkedObjectID"]
        )

        # Ensure df_filtered only includes relevant samples
        df_matched = df_filtered[df_filtered["main_objectid"].isin(df_filtered_mapping["SampleID"])]

        # Group data by main_objectid
        grouped_data = df_matched.groupby("main_objectid").apply(lambda x: {
            "objectname": x["objectname"].iloc[0],
            "created_date": x["created_date"].iloc[0],
            "updated_date": x["updated_date"].iloc[0],
            "main_objectfilepath": x["main_objectfilepath"].iloc[0],
            "linked_objects": [
                {
                    "linked_objectid": row["linked_objectid"],
                    "linked_objectfilepath": row["linked_objectfilepath"],
                    "associated_typename": row["associated_typename"]
                }
                for _, row in x.iterrows()
            ]
        }).to_dict()

        # Save results to JSON file inside save_location
        json_path = os.path.join(save_location, "query_results.json")
        with open(json_path, "w", encoding="utf-8") as json_file:
            json.dump(grouped_data, json_file, indent=4)

        # Save results to CSV file inside save_location
        output_path = os.path.join(save_location, output_filename)
        df_matched.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")

        # Step 4: Automatically Download Associated Files (Preserving Filename & Format)
        for _, row in df_matched.iterrows():
            linked_objectid = row["linked_objectid"]
            linked_objectfilepath = str(row["linked_objectfilepath"]).strip()  # Ensure it's a string

            # Validate that the filepath exists and isn't just a placeholder
            if linked_objectfilepath and not linked_objectfilepath.startswith("nan"):
                file_name = os.path.basename(linked_objectfilepath)  # Preserve original filename and extension
                file_path = os.path.join(download_folder, file_name)

                # Download file
                self.download(linked_objectid, file_path)

        print(f"All downloads completed. Files are saved in {save_location}")
        return df_matched


# Example usage:
if __name__ == "__main__":
    # initialise service_url (tenant main url)
    tenant_url = "https://crc1625.mdi.ruhr-uni-bochum.de/"  # tenant_url here

    # initialise api_key (corresponding VroApi claim must be associated with a user in database)
    api_key = ""  # your_api_key here

    client = MatInfWebApiClient(tenant_url, api_key)
   
    # Example usage
    typename_list = ['EDX CSV', 'Composition', 'SEM Image']
    sample_typename = 'Sample'
    start_date = '2024-01-01'
    end_date = '2024-12-31'
    # Elements with their percentage range (min, max)
    #element_criteria = ['Pt', 'Pd']
    element_criteria = {
    'Pt': (10, 20),  # Pt must be present, but percentage doesn't matter
    'Pd': (10, 20),   # Pd must be present, but percentage doesn't matter
    }
    df_filtered  = client.process_data(typename_list, sample_typename, start_date, end_date)

