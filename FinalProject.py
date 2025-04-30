import yaml
import logging
import arcpy
import csv
import os
from ETL.GSheetsEtl import GSheetsEtl


def setup():
    """
    Loads configuration parameters from a file or source.

    Returns:
        dict: Dictionary containing project configuration parameters.
    """
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    # configure logging
    log_path = f"{config_dict.get('proj_dir')}wnv.log"

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logging.info("Log initialized.")
    print(f"Log file created at: {log_path}")
    return config_dict

def etl():
    """
    Runs the main ETL workflow for processing spatial and attribute data.

    This function should extract, transform, and load spatial data,
    integrating with other functions as needed.
    """
    logging.debug("Entering etl method")
    logging.info("E-t-l-ing...")
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()
    logging.info("ETL processing complete")
    logging.debug("Exiting etl method")


def set_spatial_reference(config_dict):
    """
    Set spatial reference for the map document to NAD 1983 StatePlane Colorado North.

    :param config_dict: Configuration dictionary containing aprx path
    """
    logging.debug("Entering set_spatial_reference method")

    try:
        # get project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # get first map in the project
        map_doc = aprx.listMaps()[0]

        # create spatial reference for NAD 1983 StatePlane Colorado North
        # WKID: 3743 (NAD 1983 StatePlane Colorado North FIPS 0501 (US Feet))
        state_plane_noco = arcpy.SpatialReference(3743)

        # set spatial reference for map
        map_doc.spatialReference = state_plane_noco

        # save project to apply changes
        aprx.save()

        logging.info("Spatial reference set successfully to NAD 1983 StatePlane Colorado North")
    except Exception as e:
        logging.error(f"Error in set_spatial_reference: {str(e)}")
        print(f"Error in set_spatial_reference: {e}")
        raise

    logging.debug("Exiting set_spatial_reference method")


def apply_simple_renderer(config_dict):
    """
    Apply renderer to the 'Final_Analysis_Layer'
    :param config_dict: (dict): Dictionary containing project configuration parameters.
    """
    logging.debug("Entering apply_simple_renderer method")

    try:
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        for lyr in map_doc.listLayers():
            if lyr.name == "Final_Analysis_Layer":
                logging.info(f"Found layer: {lyr.name}")

                # makes sure the layer is visible
                lyr.visible = True

                if lyr.supports("SYMBOLOGY"):
                    sym = lyr.symbology

                    # symbol fill and outline color
                    sym.renderer.symbol.color = {'RGB': [255, 0, 0, 100]}
                    sym.renderer.symbol.outlineColor = {'RGB': [0, 0, 0, 100]}

                    # apply symbology, set layer transparency
                    lyr.symbology = sym
                    lyr.transparency = 50

                    logging.info("Simple renderer applied successfully")
                else:
                    logging.warning(f"Layer {lyr.name} does not support symbology")
                break
        else:
            logging.warning("Final_Analysis_Layer not found in map")

        aprx.save()

    except Exception as e:
        logging.error(f"Error in apply_simple_renderer: {str(e)}")
        print(f"Error in apply_simple_renderer: {e}")

    logging.debug("Exiting apply_simple_renderer method")


def get_field_info(config_dict, layer_name):
    """
    Helper function to get field information for a layer
    """
    logging.debug(f"Getting field info for layer: {layer_name}")
    try:
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        workspace = config_dict.get('workspace')
        field_info = []

        for lyr in map_doc.listLayers():
            if lyr.name == layer_name:
                logging.info(f"Found layer: {lyr.name}")

                if hasattr(lyr, 'dataSource'):
                    fields = arcpy.ListFields(lyr.dataSource)
                    for field in fields:
                        field_info.append(f"Field: {field.name}, Type: {field.type}, Length: {field.length}")

                    # get row count if possible
                    try:
                        count = int(arcpy.GetCount_management(lyr.dataSource).getOutput(0))
                        field_info.append(f"Row count: {count}")
                    except:
                        field_info.append("Could not get row count")
                else:
                    field_info.append("Layer has no dataSource property")

                return field_info

        return ["Layer not found"]
    except Exception as e:
        logging.error(f"Error getting field info: {str(e)}")
        return [f"Error: {str(e)}"]


def apply_definition_query(config_dict):
    """
    Apply a definition query to filter Target_Addresses that fall within Final_Analysis_Layer

    :param config_dict: Configuration dictionary containing aprx path
    """
    logging.debug("Entering apply_definition_query method")

    try:
        # get project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # get first map in the project
        map_doc = aprx.listMaps()[0]

        # get field info for debugging
        field_info = get_field_info(config_dict, "Target_Addresses")
        for info in field_info:
            logging.info(info)

        # check for proper field name and type
        has_join_count = any("Join_Count" in info for info in field_info)

        # determine the correct query based on field information
        if has_join_count:
            # try numeric query first (most likely for Join_Count)
            defQuery = "Join_Count = 1"
        else:
            # log: couldn't find the field
            logging.warning("Join_Count field not found in Target_Addresses layer")
            # try alternative field names or simplified query
            defQuery = "1=1"

        # find Target_Addresses layer
        for lyr in map_doc.listLayers():
            if lyr.name == "Target_Addresses":
                logging.info(f"Found layer for definition query: {lyr.name}")

                # make sure layer is visible
                lyr.visible = True

                # check if layer supports definition queries
                if lyr.supports("DEFINITIONQUERY"):
                    # apply definition query
                    lyr.definitionQuery = defQuery
                    logging.info(f"Definition query applied: {defQuery}")
                else:
                    logging.warning("Layer does not support definition queries")
                break
        else:
            logging.warning("Target_Addresses layer not found in map")

        # save project to apply changes
        aprx.save()

        # set map extent to data
        set_map_extent_to_data(config_dict)

    except Exception as e:
        logging.error(f"Error in apply_definition_query: {str(e)}")
        print(f"Error in apply_definition_query: {e}")

    logging.debug("Exiting apply_definition_query method")


def set_map_extent_to_data(config_dict):
    """
    Set the map extent to show all data layers
    """
    logging.debug("Entering set_map_extent_to_data method")

    try:
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        # check if we have any layers
        layers = map_doc.listLayers()
        if not layers:
            logging.warning("No layers found in map")
            return

        # zoom to the Target_Addresses layer if it exists
        target_layer = None
        for lyr in layers:
            if lyr.name == "Target_Addresses" and lyr.visible:
                target_layer = lyr
                break

        if not target_layer:
            # try Final_Analysis_Layer
            for lyr in layers:
                if lyr.name == "Final_Analysis_Layer" and lyr.visible:
                    target_layer = lyr
                    break

        if target_layer:
            logging.info(f"Would zoom to extent of {target_layer.name} (operation handled in layout's map frame)")

        else:
            logging.warning("No suitable layers found to set extent")

        aprx.save()
        logging.info("Map project saved")

    except Exception as e:
        logging.error(f"Error setting map extent: {str(e)}")

    logging.debug("Exiting set_map_extent_to_data method")

def exportMap():
    logging.debug("Entering exportMap method")

    try:
        # get project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # get first layout in project
        lyt = aprx.listLayouts()[0]

        # ask user to input sub-title
        subtitle = input("Please enter a subtitle for the map: ")
        logging.info(f"User entered subtitle: {subtitle}")

        # loop through elements to find/modify title
        for el in lyt.listElements():
            logging.debug(f"Found element: {el.name}")
            if "Title" in el.name:
                logging.info(f"Modifying title element: {el.name}")
                el.text = el.text + " - " + subtitle

        # make sure map frames are refreshed before export
        for mf in lyt.listElements("MAPFRAME_ELEMENT"):
            logging.info(f"Refreshing map frame: {mf.name}")
            mf.camera.setExtent(mf.getLayerExtent(mf.map.listLayers()[0], False, True))

        # export map to PDF
        output_pdf = f"{config_dict.get('proj_dir')}WestNileOutbreakMap.pdf"
        logging.info(f"Exporting map to: {output_pdf}")
        lyt.exportToPDF(output_pdf, resolution=300)

        logging.info("Map exported successfully")
    except Exception as e:
        logging.error(f"Error in exportMap: {str(e)}")
        print(f"Error in exportMap: {e}")

    logging.debug("Exiting exportMap method")


def print_layer_info(config_dict):
    """
    Debug function to print information about all layers in the map
    """
    try:
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        logging.info("=== Layer Information ===")
        for lyr in map_doc.listLayers():
            logging.info(f"Layer name: {lyr.name}")
            logging.info(f"  Visible: {lyr.visible}")

            if hasattr(lyr, 'dataSource'):
                logging.info(f"  Data source: {lyr.dataSource}")
                try:
                    count = int(arcpy.GetCount_management(lyr.dataSource).getOutput(0))
                    logging.info(f"  Feature count: {count}")
                except:
                    logging.info("  Could not get feature count")
            else:
                logging.info("  No data source")

            if lyr.supports("DEFINITIONQUERY"):
                logging.info(f"  Definition query: {lyr.definitionQuery}")

            logging.info("  ---")
    except Exception as e:
        logging.error(f"Error printing layer info: {str(e)}")

def generate_address_report(config_dict):
    """
    Generates a CSV report of addresses within the final_analysis buffer area.
    Extracts records from 'Target_Addresses' layer where Join_Count = 1 (inside final_analysis layer),
    and writes selected fields to a CSV file.
    """
    try:
        print("Generating address report...")
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        # find Target_Addresses layer
        target_layer = None
        for lyr in map_doc.listLayers():
            if lyr.name == "Target_Addresses":
                target_layer = lyr
                break

        if not target_layer:
            print("Target_Addresses layer not found.")
            return

        # create feature layer with only selected addresses
        query = '"Join_Count" = 1'
        arcpy.management.MakeFeatureLayer(target_layer, "selected_addresses", query)

        # define output CSV path
        output_csv = os.path.join(config_dict.get('proj_dir'), "WNV_spraying_addresses.csv")

        # export fields
        fields = ["OBJECTID_1", "FULLADDR", "ADDRNUM", "STREETNAME", "STREETSUFF"]

        with open(output_csv, "w", newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(fields)  # Write header

            with arcpy.da.SearchCursor("selected_addresses", fields) as cursor:
                for row in cursor:
                    writer.writerow(row)

        print(f"Address report generated: {output_csv}")

    except Exception as e:
        print(f"Error in generate_address_report method: {e}")


if __name__ == "__main__":
    global config_dict
    config_dict = setup()
    logging.info("Starting West Nile Virus Simulation")
    logging.info(f"Config Dict: {config_dict}")
    etl()
    set_spatial_reference(config_dict)
    apply_simple_renderer(config_dict)
    print_layer_info(config_dict)
    apply_definition_query(config_dict)
    set_map_extent_to_data(config_dict)
    exportMap()
    generate_address_report(config_dict)
