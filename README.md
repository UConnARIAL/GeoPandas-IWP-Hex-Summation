# GeoPandas-IWP-Hex-Summation
Summarize count of ice-wedge polygons within hexagonal grid cells with a CPU-parallelized GeoPandas-based algorithm.

IWP hexagonal summation algorithm
There are 3 files required before running the algorithm that can be quickly created in GIS software:
1)	Shapefile of all footprints merged together with overlaps removed. 
i.	I created this by loading all of the footprint files from "F:\pan_arctic_master_copy\footprints_dissolved" into QGIS and performing a merge operation. 
ii.	Then, I installed the ProcessX plugin and used the “Remove Self-Overlapping Portions by Condition” tool with the default settings. This removes all overlaps from the merged footprint layer. 
iii.	However, some footprints get left behind for an unknown reason. To fix this, I just take the merged footprint layer and erase the areas that intersect with the new overlap-removed footprint layer, then merge the erased result with the overlap-removed footprint layer to fill in the gaps. 
iv.	To be safe, run the Repair Geometry tool in ArcGIS Pro. This takes less than 5 minutes.
2)	Shapefile of hexagonal grid covering the extent of the overlap-removed merged footprint layer (however, create a file geodatabase feature class copy of this as well). This can be created in ArcGIS Pro using the Generate Tessellation tool. Each hexagon will have its own unique string identifier (e.g., Grid_ID).
3)	CSV lookup table matching each hexagon Grid_ID to footprints that intersect with the given hexagon. This can be created in ArcGIS by performing a spatial join on the hexagonal grid. 
i.	But first, you should have a geodatabase feature class version of the hexagonal grid, which does not have any length limitations on field length. 
ii.	Join the footprint layer to the hexagonal grid based on intersection. In the field map, choose the field that holds the footprint name, select the Concatenate rule, and set length to a large number (e.g., 1,000,000) to hold all of the strings that will be concatenated. This will update the table of the hexagonal grid with a field (e.g., Filenames) holding the names (separated by commas) of all the footprints that intersect with each hexagon. 
iii.	Then you can export the Grid_ID and Filenames fields of the hexagonal grid table into a CSV file.
With these files, you can begin the IWP summation algorithm which follows this workflow:
1)	Load in the hexagon-footprint CSV table into a dictionary, where the key will be the Grid_ID of each hexagon and the values will be the names of the intersecting footprints. These footprint names are the same as the actual IWP shapefile names, except you simply need to append "_u16rf3413_pansh.shp".
2)	Initialize a dictionary to hold the IWP counts within each hexagon.
3)	In an outer loop, iterate over each hexagon in the hexagonal grid shapefile. Look up the Grid_ID in the dictionary and retrieve the list of IWP shapefiles that need to be processed (footprint name + "_u16rf3413_pansh.shp".) 
4)	In the inner loop, traverse through the IWP shapefile directory (“F:\pan_arctic_master_copy\iwp_files”) to find those filenames that match the filenames retrieved for the given hexagon. Once found, load the matching footprint from the footprint shapefile. Perform a spatial join using the within operation to select only those IWPs that fall within the overlap-removed footprint. From this selection, perform another spatial join to select those IWPs that are within the hexagon. 
5)	Add up the count of IWP polygons found across each processed file for a given hexagon and add it to results dictionary where the Grid_ID is the key.
6)	Once all hexagon grid cells are processed, write the dictionary to a CSV file.
