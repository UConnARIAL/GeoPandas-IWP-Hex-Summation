import os
import geopandas as gpd
import pandas as pd
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
                            # print(f"Processing file {file}...")
                            # Read in the IWP shapefile
                            iwp_shapefile = gpd.read_file(full_name)
                            # Extract centroids of IWPs and store in GeoDataFrame
                            iwp_center = gpd.GeoDataFrame(geometry=iwp_shapefile.geometry.centroid, crs='epsg:3413')

                            # Isolate the row in the footprint GeoDataFrame where "Name" matches the IWP shapefile name
                            footprint_row = footprint[footprint['Name'] == file]

                            # Select only those features in the IWP shapefile that intersect with the isolated footprint
                            iwp_within_footprint = gpd.sjoin(iwp_center, footprint_row, predicate='within')
                            # The following is a fix to a known problem with the particular geopandas version being used
                            iwp_within_footprint = iwp_within_footprint.drop(['index_right'], axis=1)

                            # Select those that fall within the hexagon
                            iwp_within_hexagon = gpd.sjoin(iwp_within_footprint, hexagon_gdf, predicate='within')
                            # # Print the number of polygons found within the IWP shapefile
                            # print(f"Number of polygons found in {file}: {len(iwp_within_hexagon)}")

                            # Increment the count of ice wedge polygons for the hexagon cell
                            ice_wedge_count += len(iwp_within_hexagon)
    # Calculate the processing time for the current hexagon
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"Processing time for hexagon cell {grid_id}: {processing_time} seconds")

    return ice_wedge_count

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

    # Aggregate the results
    total_ice_wedge_count = sum(results)
    print(f"Total ice wedge count across all hexagons: {total_ice_wedge_count}")

    # Save the results to a CSV file
    output_csv_path = "D:/manuscripts/IWP_data_paper/ice_wedge_counts.csv"
    print("Saving ice wedge counts to CSV file...")
    output_df = pd.DataFrame({"Grid_ID": hexagon_grid['arctic_h3h'], "Ice_Wedge_Count": results})
    output_df.to_csv(output_csv_path, index=False)

    print("Ice wedge counts saved to:", output_csv_path)
