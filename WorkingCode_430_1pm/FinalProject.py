import yaml
import logging
import arcpy
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
        # Get the project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # Get the first map in the project
        map_doc = aprx.listMaps()[0]

        # Create spatial reference for NAD 1983 StatePlane Colorado North FIPS 0501 (US Feet)
        # WKID: 3743 (NAD 1983 StatePlane Colorado North FIPS 0501 (US Feet))
        state_plane_noco = arcpy.SpatialReference(3743)

        # Set the spatial reference for the map
        map_doc.spatialReference = state_plane_noco

        # Save the project to apply changes
        aprx.save()

        logging.info("Spatial reference set successfully to NAD 1983 StatePlane Colorado North")
    except Exception as e:
        logging.error(f"Error in set_spatial_reference: {str(e)}")
        print(f"Error in set_spatial_reference: {e}")
        raise

    logging.debug("Exiting set_spatial_reference method")


def apply_simple_renderer(config_dict):
    """
    Apply a simple renderer to the 'Final_Analysis_Layer'
    :param config_dict: (dict): Dictionary containing project configuration parameters.
    """
    logging.debug("Entering apply_simple_renderer method")

    try:
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))
        map_doc = aprx.listMaps()[0]

        for lyr in map_doc.listLayers():
            if lyr.name == "Final_Analysis_Layer":
                logging.info(f"Found layer: {lyr.name}")

                # First, make sure the layer is visible
                lyr.visible = True

                if lyr.supports("SYMBOLOGY"):
                    sym = lyr.symbology

                    # Set symbol fill and outline color
                    sym.renderer.symbol.color = {'RGB': [255, 0, 0, 100]}
                    sym.renderer.symbol.outlineColor = {'RGB': [0, 0, 0, 100]}

                    # Apply symbology and set layer transparency
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

                    # Get row count if possible
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
        # Get the project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # Get the first map in the project
        map_doc = aprx.listMaps()[0]

        # Get field info to help debug
        field_info = get_field_info(config_dict, "Target_Addresses")
        for info in field_info:
            logging.info(info)

        # Check for the proper field name and type
        has_join_count = any("Join_Count" in info for info in field_info)

        # Determine the correct query based on field information
        if has_join_count:
            # Try numeric query first (most likely for Join_Count)
            defQuery = "Join_Count = 1"
        else:
            # Log that we couldn't find the field
            logging.warning("Join_Count field not found in Target_Addresses layer")
            # Try some alternative field names or a simplified query
            defQuery = "1=1"  # This will show all records for now

        # Find the Target_Addresses layer
        for lyr in map_doc.listLayers():
            if lyr.name == "Target_Addresses":
                logging.info(f"Found layer for definition query: {lyr.name}")

                # Make sure the layer is visible
                lyr.visible = True

                # Check if layer supports definition queries
                if lyr.supports("DEFINITIONQUERY"):
                    # Apply the definition query
                    lyr.definitionQuery = defQuery
                    logging.info(f"Definition query applied: {defQuery}")
                else:
                    logging.warning("Layer does not support definition queries")
                break
        else:
            logging.warning("Target_Addresses layer not found in map")

        # Save the project to apply changes
        aprx.save()

        # Set map extent to data
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

        # Check if we have any layers
        layers = map_doc.listLayers()
        if not layers:
            logging.warning("No layers found in map")
            return

        # Try to zoom to the Target_Addresses layer if it exists
        target_layer = None
        for lyr in layers:
            if lyr.name == "Target_Addresses" and lyr.visible:
                target_layer = lyr
                break

        if not target_layer:
            # Try Final_Analysis_Layer
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
        # Get the project object
        aprx = arcpy.mp.ArcGISProject(config_dict.get('aprx'))

        # Get first layout in the project
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

        # Make sure map frames are refreshed before export
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


if __name__ == "__main__":
    global config_dict
    config_dict = setup()
    logging.info("Starting West Nile Virus Simulation")
    logging.info(f"Config Dict: {config_dict}")
    etl()
    set_spatial_reference(config_dict)
    apply_simple_renderer(config_dict)
    print_layer_info(config_dict)  # Add this to debug layer information
    apply_definition_query(config_dict)
    set_map_extent_to_data(config_dict)  # Make sure map is zoomed to show data
    exportMap()