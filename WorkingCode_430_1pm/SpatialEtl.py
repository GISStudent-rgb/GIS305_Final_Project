
class SpatialEtl:
    """
    A base class for performing ETL operations on spatial data.

    Attributes:
        config_dict (dict): Configuration dictionary containing project parameters.
        data_format (str): The format of the data being processed.
        destination (str): The workspace or destination for loaded data.
    """
    def __init__(self, config_dict):
        """
        Initializes the SpatialEtl object with the provided configuration.

        Args:
            config_dict (dict): Configuration settings including data format,
                                destination workspace, and other parameters.
        """
        self.config_dict = config_dict
        self.data_format = config_dict.get('data_format')
        self.destination = config_dict.get('workspace')

    def extract(self):
        """
        Extracts data from a remote source to a local directory.
        """
        print(f"Extracting data from {self.config_dict.get('remote_url')} to {self.config_dict.get('proj_dir')}")

    def transform(self):
        """
        Transforms extracted data into a suitable format for loading.
        """
        print(f"Transforming {self.data_format}")

    def load(self):
        """
        Loads transformed data into the destination workspace.
        """
        print(f"Loading data into {self.destination}")

