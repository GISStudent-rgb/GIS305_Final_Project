import arcpy
import requests
import csv
import urllib.parse

from ETL.SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):
    """
    A subclass of SpatialEtl for processing address data from a Google Sheets CSV.

    This class extracts address records from a Google Sheets URL, transforms them by
    geocoding to XY coordinates, and loads them into a spatial feature class.

    Attributes:
        config_dict (dict): Configuration dictionary containing project parameters.
    """
    config_dict = None

    def __init__(self, config_dict):
        """
        Initializes the GSheetsEtl object with the provided configuration.

        Args:
            config_dict (dict): Configuration settings including Google Sheets URL,
                                geocoding service details, project directory, and workspace.
        """
        super().__init__(config_dict)

    def extract(self):
        """
        Extracts address records from a Google Sheets URL and saves them locally as a CSV.

        Returns:
            str: Path to the saved CSV file containing the extracted addresses.
        """
        print("Extracting addresses from google spreadsheet")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
            output_file.write(data)

        print("Extraction complete")
        return f"{self.config_dict.get('proj_dir')}new_addresses_Lab2.csv"

    def transform(self):
        """
        Transforms extracted address data by geocoding each address to XY coordinates.

        The transformed results are saved as a new CSV file suitable for use with
        ArcGIS XY Table To Point tool.
        """
        print("Transforming addresses for geocoding...")

        output_path = f"{self.config_dict.get('proj_dir')}new_addresses_Lab2.csv"
        input_path = f"{self.config_dict.get('proj_dir')}addresses.csv"

        with open(output_path, "w", newline='') as transformed_file:
            writer = csv.writer(transformed_file)
            writer.writerow(["X", "Y", "Type"])

            with open(input_path, "r", encoding="utf-8") as partial_file:
                csv_dict = csv.DictReader(partial_file, delimiter=',')

                for row in csv_dict:
                    raw_address = row.get("Street Address", "").strip()

                    address = f"{raw_address}, Boulder, CO"
                    print(f"Geocoding: {address}")

                    encoded_address = urllib.parse.quote_plus(address)
                    geocode_url = f"{self.config_dict.get('geocoder_prefix_url')}{encoded_address}{self.config_dict.get('geocoder_suffix_url')}"

                    try:
                        r = requests.get(geocode_url)
                        r.raise_for_status()
                        resp_dict = r.json()

                        matches = resp_dict.get('result', {}).get('addressMatches', [])
                        if matches:
                            coords = matches[0]['coordinates']
                            x = float(coords['x'])
                            y = float(coords['y'])
                            writer.writerow([x, y, "Residential"])
                            print(f"Success: {x}, {y}")
                        else:
                            print(f"No match found for: {address}")
                    except Exception as e:
                        print(f"Unexpected error for {address}: {e}")

        print("Transform complete.")

    def load(self):
        """
        Loads transformed XY coordinate data into a point feature class in ArcGIS.

        Reads the geocoded CSV file and creates a feature class named 'avoid_points'
        in the configured workspace.
        """
        print("Loading data")
        arcpy.env.workspace = self.config_dict.get('workspace')
        arcpy.env.overwriteOutput = True

        in_table = f"{self.config_dict.get('proj_dir')}new_addresses_Lab2.csv"
        out_feature_class = "avoid_points"
        x_coords = "X"
        y_coords = "Y"

        print(f"Checking CSV file: {in_table}")
        try:
            with open(in_table, 'r') as csv_file:
                print("CSV preview:")
                for i, line in enumerate(csv_file):
                    print(line.strip())
                    if i >= 4:
                        break
        except Exception as e:
            print(f"Error reading CSV: {e}")

        try:
            arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)
            print(f"Total Points Created: {arcpy.GetCount_management(out_feature_class)}")
        except Exception as e:
            print(f"Error in XYTableToPoint: {e}")
            fields = arcpy.ListFields(in_table)
            for field in fields:
                print(f"Field: {field.name}, Type: {field.type}")

    def process(self):
        """
        Runs the full ETL process: extract, transform, load in sequence.
        """
        self.extract()
        self.transform()
        self.load()
