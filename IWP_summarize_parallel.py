import os
import geopandas as gpd
import pandas as pd
import numpy as np
import time
from multiprocessing import Pool, freeze_support

# Define the root directory containing all IWP shapefiles
root_dir = r'F:\pan_arctic_master_copy\iwp_files'

# Read the CSV file into a dictionary.
csv_file_path = "D:/manuscripts/IWP_data_paper/hex_join_dict.csv"
print("Reading CSV file...")
csv_data = pd.read_csv(csv_file_path)
grid_files_dict = csv_data.set_index('Grid_ID')['Filenames'].to_dict()

# Read in the footprint file and append "_u16rf3413_pansh" to the "Name" field
footprint_shapefile_path = r"D:\manuscripts\IWP_data_paper\merged_FPs_noOverlap_final_1.shp"
print("Reading footprint shapefile...")
footprint = gpd.read_file(footprint_shapefile_path)
footprint['Name'] = footprint['Name'] + "_u16rf3413_pansh.shp"

# Define the function to process each hexagon
def process_hexagon(args):
    index, hexagon_row, total_hexagons = args
    grid_id = hexagon_row['arctic_h3h']
    filenames = [filename + "_u16rf3413_pansh.shp" for filename in str(grid_files_dict.get(grid_id, '')).split(',') if
                 filename.strip()]  # Append to filenames
    print(f"Processing hexagon cell {grid_id} ({index + 1}/{total_hexagons})...")

    # Time the processing for each hexagon
    start_time = time.time()

    # Initialize variables to store aggregated attributes
    total_area = 0
    total_perimeter = 0
    total_length = 0
    total_width = 0
    ice_wedge_count = 0  # Initialize ice wedge count for this hexagon

    # Create a single-row GeoDataFrame with the hexagon geometry
    hexagon_gdf = gpd.GeoDataFrame(geometry=[hexagon_row.geometry], crs='epsg:3413')

    # Inner loop traversing through the master directory containing all the IWP shapefiles
    for root, dirs, files in os.walk(root_dir):
        for d in dirs:
            if d.endswith('iwp'):
                process_folder = os.path.join(root, d)
                for root2, dirs2, files2 in os.walk(process_folder):
                    for file in files2:
                        full_name = os.path.join(root2, file)
                        if file.endswith('.shp') and file in filenames:
                            # Read in the IWP shapefile
                            iwp_shapefile = gpd.read_file(full_name)
                            # Extract centroids of IWPs and store in GeoDataFrame
                            iwp_centroids = gpd.GeoDataFrame(geometry=iwp_shapefile.geometry.centroid, crs='epsg:3413')
                            iwp_centroids = iwp_centroids.assign(**iwp_shapefile.drop(columns='geometry').iloc[:, 1:].to_dict())

                            # Isolate the row in the footprint GeoDataFrame where "Name" matches the IWP shapefile name
                            footprint_row = footprint[footprint['Name'] == file]

                            # Select only those features in the IWP shapefile that intersect with the isolated footprint
                            iwp_within_footprint = gpd.sjoin(iwp_centroids, footprint_row, predicate='within')
                            # The following is a fix to a known problem with the particular geopandas version being used
                            iwp_within_footprint = iwp_within_footprint.drop(['index_right'], axis=1)

                            # Select those that fall within the hexagon
                            iwp_within_hexagon = gpd.sjoin(iwp_within_footprint, hexagon_gdf, predicate='within')

                            # Aggregate attributes
                            total_area += iwp_within_hexagon['Area'].sum()
                            total_perimeter += iwp_within_hexagon['Perimeter'].sum()
                            total_length += iwp_within_hexagon['Length'].sum()
                            total_width += iwp_within_hexagon['Width'].sum()

                            # Increment the count of ice wedge polygons for the hexagon cell
                            ice_wedge_count += len(iwp_within_hexagon)

    # Calculate the mean values for the attributes
    mean_area = total_area / ice_wedge_count if ice_wedge_count > 0 else 0
    mean_perimeter = total_perimeter / ice_wedge_count if ice_wedge_count > 0 else 0
    mean_length = total_length / ice_wedge_count if ice_wedge_count > 0 else 0
    mean_width = total_width / ice_wedge_count if ice_wedge_count > 0 else 0
    mean_compactness = ((total_perimeter ** 2) / (4 * np.pi * total_area))/ ice_wedge_count if total_area > 0 else 0

    # Calculate the processing time for the current hexagon
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"Processing time for hexagon cell {grid_id}: {processing_time} seconds")

    return grid_id, ice_wedge_count, mean_area, mean_perimeter, mean_length, mean_width, mean_compactness

if __name__ == '__main__':
    # Add freeze_support() to support freezing the executable on Windows
    freeze_support()

    # Read in the hexagon grid shapefile
    hexagon_shapefile_path = r"D:\manuscripts\IWP_data_paper\arctic_h3hex_res5_proj.shp"
    print("Reading hexagon grid shapefile...")
    hexagon_grid = gpd.read_file(hexagon_shapefile_path)

    # Determine the total number of hexagon cells
    total_hexagons = len(hexagon_grid)
    print(f"Total number of hexagon cells: {total_hexagons}")

    # Create a pool of worker processes
    num_processes = os.cpu_count() or 1
    print(f"Number of CPU cores available: {num_processes}")
    pool = Pool(processes=num_processes)

    # Map the processing function to each hexagon row in parallel
    results = pool.map(process_hexagon, [(index, hexagon_row, total_hexagons) for index, hexagon_row in hexagon_grid.iterrows()], chunksize=None)

    # Close the pool to release resources
    pool.close()
    pool.join()

    # Unpack the results
    grid_ids, ice_wedge_counts, mean_areas, mean_perimeters, mean_lengths, mean_widths, mean_compactnesses = zip(*results)

    # Save the results to a CSV file
    output_csv_path = "D:/manuscripts/IWP_data_paper/ice_wedge_attributes.csv"
    print("Saving ice wedge attributes to CSV file...")
    output_df = pd.DataFrame({
        "Grid_ID": grid_ids,
        "Ice_Wedge_Count": ice_wedge_counts,
        "Mean_Area": mean_areas,
        "Mean_Perimeter": mean_perimeters,
        "Mean_Length": mean_lengths,
        "Mean_Width": mean_widths,
        "Compactness": mean_compactnesses
    })
    output_df.to_csv(output_csv_path, index=False)

    print("Ice wedge attributes saved to:", output_csv_path)

# Test on a few hexagons
# if __name__ == '__main__':
#     # Add freeze_support() to support freezing the executable on Windows
#     freeze_support()
#
#     # Read in the hexagon grid shapefile
#     hexagon_shapefile_path = r"D:\manuscripts\IWP_data_paper\arctic_h3hex_res5_proj.shp"
#     print("Reading hexagon grid shapefile...")
#     hexagon_grid = gpd.read_file(hexagon_shapefile_path)
#
#     # Choose three specific hexagon cells from the hexagon grid
#     selected_hexagon_rows = hexagon_grid.iloc[:3]
#
#     results = []
#
#     for index, selected_hexagon_row in selected_hexagon_rows.iterrows():
#         # Call the processing function with the selected hexagon cell
#         result = process_hexagon((index, selected_hexagon_row, len(selected_hexagon_rows)))
#         results.append(result)
#
#     # Unpack the results
#     grid_ids, ice_wedge_counts, mean_areas, mean_perimeters, mean_lengths, mean_widths, mean_compactnesses = zip(
#         *results)
#
#     # Save the results to a single CSV file
#     output_csv_path = "D:/manuscripts/IWP_data_paper/ice_wedge_attributes_test.csv"
#     print("Saving ice wedge attributes for multiple hexagon cells to CSV file...")
#     output_df = pd.DataFrame({
#         "Grid_ID": grid_ids,
#         "Ice_Wedge_Count": ice_wedge_counts,
#         "Mean_Area": mean_areas,
#         "Mean_Perimeter": mean_perimeters,
#         "Mean_Length": mean_lengths,
#         "Mean_Width": mean_widths,
#         "Mean_Compactness": mean_compactnesses
#     })
#     output_df.to_csv(output_csv_path, index=False)
#
#     print("Ice wedge attributes for multiple hexagon cells saved to:", output_csv_path)

