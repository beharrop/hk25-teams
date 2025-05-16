# Convert ERA5 AR tracks into healpix zarr files
# By Lexie Goldberger

import s3fs
import math
import xarray as xr
import pandas as pd
import numpy as np
import os
import re
import pyproj
import intake
from easygems import healpix as egh
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.lines as mlines
# import metpy
import datetime
import cartopy.crs as ccrs
from functools import partial
import healpy
import zarr
import xesmf as xe
import numcodecs
import gc
import warnings
warnings.filterwarnings('ignore')

# ERA5 mask
in_mask_dir = '/pscratch/sd/b/beharrop/people/for_lexie/ERA5_AR_tracks.nc'
print(f" you are processing ERA5 AR track masks into healpix gridded ZARR files with {in_mask_dir}")
dshp_ar = xr.open_dataset(in_mask_dir)
dshp_ar = dshp_ar.rename({'latitude': 'lat', 'longitude': 'lon'})

signed_lon = True if np.min(dshp_ar.lon) < 0 else False
print(f"Dataset lon coordinate has negative values: {signed_lon}")

print('grabbing healpix data to grid to')
# List available catalogs
catalog_file = "https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml"
# Select a catalog for your location
current_location = "NERSC"
cat = intake.open_catalog(catalog_file)[current_location]
cat = intake.open_catalog("https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml")[current_location]

# Get zoom level from catalog
highest_zoom_level = 8# pd.DataFrame(cat["scream2D_hrly"].describe()["user_parameters"]).allowed.max()[0]
catalog_params = {'zoom': highest_zoom_level}  # Can have multiple parameters
print(f"zoom level used {highest_zoom_level}")
# Note the use of **catalog_params to pass the parameters
ds_hp = cat['scream2D_hrly'](**catalog_params).to_dask()

# # Convert floats in cell
# if "cell" in ds_hp.coords:
#     ds_hp["cell"] = ds_hp["cell"].astype("int64")

# Add lat/lon coordinates to the DataSet.
# Set signed_lon=True for matching lat/lon DataSet with longitude -180 to +180
ds_hp = ds_hp.pipe(partial(egh.attach_coords,signed_lon=signed_lon))

# Assign extra coordinates (lon_hp, lat_hp) to the HEALPix coordinates
# This is needed for limiting the extrapolation during remapping
lon_hp = ds_hp.lon.assign_coords(cell=ds_hp.cell, lon_hp=lambda da: da)
lat_hp = ds_hp.lat.assign_coords(cell=ds_hp.cell, lat_hp=lambda da: da)

print('grabbed healpix data to grid to')

def fix_coords(ds, lat_dim="lat", lon_dim="lon", roll=False):
    """
    Fix coordinates in a dataset:
    1. Convert longitude from -180/+180 to 0-360 range (optional)
    2. Roll dataset to start at longitude 0 (optional)
    3. Ensure coordinates are in ascending order
    
    Parameters:
    -----------
    ds : xarray.Dataset or xarray.DataArray
        Dataset with lat/lon coordinates
    lat_dim : str, optional
        Name of latitude dimension, default "lat"
    lon_dim : str, optional
        Name of longitude dimension, default "lon"
    roll : bool, optional, default=False
        If True, convert longitude from -180/+180 to 0-360, and roll the dataset to start at longitude 0
        
    Returns:
    --------
    xarray.Dataset or xarray.DataArray
        Dataset with fixed coordinates
    """
    if roll:
        # Find where longitude crosses from negative to positive (approx. where lon=0)
        lon_0_index = (ds[lon_dim] < 0).sum().item()
        
        # Create indexers for the roll
        lon_indices = np.roll(np.arange(ds.sizes[lon_dim]), -lon_0_index)
        
        # Roll dataset and convert longitudes to 0-360 range
        ds = ds.isel({lon_dim: lon_indices})
        lon360 = xr.where(ds[lon_dim] < 0, ds[lon_dim] + 360, ds[lon_dim])
        ds = ds.assign_coords({lon_dim: lon360})
    
    # Ensure latitude and longitude are in ascending order if needed
    if np.all(np.diff(ds[lat_dim].values) < 0):
        ds = ds.isel({lat_dim: slice(None, None, -1)})
    if np.all(np.diff(ds[lon_dim].values) < 0):
        ds = ds.isel({lon_dim: slice(None, None, -1)})
    
    return ds

def calculate_healpix_tolerance(zoom_level):
    """
    Calculate appropriate tolerance for is_valid function based on HEALPix zoom level.
    Returns approximately one grid cell size in degrees.
    
    Args:
        zoom_level (int): HEALPix zoom level
        
    Returns:
        float: Tolerance in degrees
    """
    # Calculate nside from zoom level (nside = 2^zoom)
    # nside determines HEALPix resolution - each increase in zoom doubles the resolution
    nside = 2 ** zoom_level
    
    # Calculate approximate pixel size in degrees
    # Mathematical derivation:
    # - Sphere has total area of 4π steradians (= 4π × (180/π)² sq. degrees)
    # - HEALPix divides sphere into 12 × nside² equal-area pixels
    # - Each pixel has area = 4π × (180/π)² / (12 × nside²) sq. degrees
    # - Linear size = √(pixel area) ≈ 58.6 / nside degrees
    # This gives approximately the angular width of one HEALPix cell
    pixel_size_degrees = 58.6 / nside
    
    return pixel_size_degrees # tolerance


def is_valid(ds, tolerance):
    """
    Limit extrapolation distance to a certain tolerance.
    This is useful for preventing extrapolation of regional data to global HEALPix grid.

    Args:
        ds (xarray.Dataset):
            The dataset containing latitude and longitude coordinates.
        tolerance (float): default=0.1
            The maximum allowed distance in [degrees] for extrapolation.

    Returns:
        xarray.DataSet.
    """
    return (np.abs(ds.lat - ds.lat_hp) < tolerance) & (np.abs(ds.lon - ds.lon_hp) < tolerance)


 # Remap mask DataSet to HEALPix
print('started remapping dataset to healpix grid')
tolerance = calculate_healpix_tolerance(highest_zoom_level)
dshp_ar_hp = dshp_ar.pipe(fix_coords).sel(lon=lon_hp, lat=lat_hp, method="nearest").where(partial(is_valid, tolerance=tolerance))
# Drop unnecessary coordinates
dshp_ar_hp = dshp_ar_hp.drop_vars(["lat_hp", "lon_hp","lon","lat"])
print('finished remapping dataset to healpix grid')

# Single Precision for Floats

def get_dtype(da):
    if np.issubdtype(da.dtype, np.floating):
        return "float32"
    else:
        return da.dtype
    
    
# Chunking (note, 'cell' has to match name of column dimension in input)

def get_chunks(dimensions):
    if "level" in dimensions:
        chunks = {
            "time": 24,
            "cell": 4**5,
            "level": 4,
        }
    else:
        chunks = {
            "time": 24,
            "cell": 4**6,
        }

    return tuple((chunks[d] for d in dimensions))

# Compression

def get_compressor():
    return numcodecs.Blosc("zstd", shuffle=2)

#Loop over all variables and create encoding directory...

def get_encoding(dataset):
    return {
        var: {
            "compressor": get_compressor(),
            "dtype": get_dtype(dataset[var]),
            "chunks": get_chunks(dataset[var].dims),
        }
        for var in dataset.variables
        if var not in dataset.dims
    }

# Set base directory and base version
out_dir = '/pscratch/sd/l/lexberg/hackathon2025/ARtracks_ERA5_hpZARR_data/'
base_version = 'v1'
print(f'will save data in {out_dir}')
# Extract base prefix and number
prefix = base_version[0]
version_num = int(base_version[1:])

# List files/directories in output directory
existing = os.listdir(out_dir)

# Extract version numbers from filenames
version_nums = []
pattern = re.compile(r'_v(\d+)\.zarr$')

for filename in existing:
    match = pattern.search(filename)
    if match:
        version_nums.append(int(match.group(1)))

# Find highest version and set next version
next_version = max(version_nums, default=0) + 1
version = f'v{next_version}'

print("Next version to save zarr file to:", version)

# Now Loop (max memory is still 25GB for zoom =7 Not using dask right or the assigment should be different...)
dn = dshp_ar_hp
for x in range(highest_zoom_level-1,-1,-1):
    s = str(x)
    # ofn = "scream_ne1024_all_hp"+s+"_v6.zarr"
    ofn = f"{out_dir}_all_hp{s}_{version}.zarr"
    # Coarsen the dataset
    dx = dn.coarsen(cell=4).mean()
    # Update HEALPix level metadata
    dx['crs'].attrs['healpix_nside'] = 2**int(s)
    # Write to Zarr
    store = zarr.storage.DirectoryStore(ofn, dimension_separator="/")
    try:
        dx.chunk({"time": 24, "cell": -1}).to_zarr(store, encoding=get_encoding(dx))
        print(f"✓ Wrote to: {ofn}")
    except:
        pass
    # Update dataset with the new coarsened data
    dn = dx
    del dx,store
    gc.collect()







