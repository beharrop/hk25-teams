### Get HRRR data, remap to healpix
### Save as zarr files

# By Lexie Goldberger

#####################################################################################################
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
import metpy
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

#####################################################################################################
# EDIT VARIABLES

# Get times you want data for
selected_times = xr.cftime_range(start='2019-08-14', periods=24, freq='H', calendar='noleap') #periods=365*24 # hrs in a year
# set file convention
# Set base directory and base version
out_dir = '/pscratch/sd/l/lexberg/hackathon2025/HRRR_hpZARR_data/'
base_version = 'v1'

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
#####################################################################################################
# Functions to Get HRRR Data 
projection = ccrs.LambertConformal(central_longitude=262.5, 
                                   central_latitude=38.5, 
                                   standard_parallels=(38.5, 38.5),
                                    globe=ccrs.Globe(semimajor_axis=6371229,
                                                     semiminor_axis=6371229))
s3 = s3fs.S3FileSystem(anon=True)
def lookup(path):
    return s3fs.S3Map(path, s3=s3)
def load_dataset(urls):
    fs = s3fs.S3FileSystem(anon=True)
    ds = xr.open_mfdataset([s3fs.S3Map(url, s3=fs) for url in urls], engine='zarr')
    ds = ds.rename(projection_x_coordinate="x", projection_y_coordinate="y")
    ds = ds.metpy.assign_crs(projection.to_cf())
    ds = ds.metpy.assign_latitude_longitude()
    ds = ds.set_coords("time")
    return ds
def load_combined_dataset(selected_times, level, param_short_name):
    combined_ds = None
    for ts in selected_times:
        time = ts
        group_url = time.strftime(f"s3://hrrrzarr/sfc/%Y%m%d/%Y%m%d_%Hz_anl.zarr/{level}/{param_short_name}")
        subgroup_url = f"{group_url}/{level}"
        partial_ds = load_dataset([group_url, subgroup_url])
        if not combined_ds:
            combined_ds = partial_ds
        else:
            combined_ds = xr.concat([combined_ds, partial_ds], dim="time", combine_attrs="drop_conflicts")
    return combined_ds

#####################################################################################################
# Get Data
print('grabbing HRRR data')
dshrrr_u = load_combined_dataset(selected_times, '10m_above_ground', 'UGRD'); dshrrr_u = dshrrr_u.astype('float32')
dshrrr_v = load_combined_dataset(selected_times, '10m_above_ground', 'VGRD'); dshrrr_v = dshrrr_v.astype('float32')

dshrrr_rh = load_combined_dataset(selected_times, '2m_above_ground', 'RH'); dshrrr_rh = dshrrr_rh.astype('float32')
dshrrr_tmp = load_combined_dataset(selected_times, '2m_above_ground', 'TMP'); dshrrr_tmp = dshrrr_tmp.astype('float32')
dshrrr_dpt = load_combined_dataset(selected_times, '2m_above_ground', 'DPT'); dshrrr_dpt = dshrrr_dpt.astype('float32')
dshrrr_pot = load_combined_dataset(selected_times, '2m_above_ground', 'POT'); dshrrr_pot = dshrrr_pot.astype('float32')
dshrrr_spfh = load_combined_dataset(selected_times, '2m_above_ground', 'SPFH'); dshrrr_spfh = dshrrr_spfh.astype('float32')

ws_data = np.sqrt(dshrrr_u.UGRD**2 + dshrrr_v.VGRD**2).astype('float32')
dshrrr_ws = xr.DataArray(ws_data, coords=dshrrr_u.coords, dims=dshrrr_u.dims, name='ws')
dshrrr_ws = xr.Dataset({'ws': dshrrr_ws})
dshrrr_ws.attrs['units'] = 'm/s'
dshrrr_ws.attrs['long_name'] = 'HRRR Wind Speed Magnitude 10m above surface'
print('grabbed HRRR data')
#####################################################################################################
# Regrid Data
def regriddata(dshrrr):
    input_grid = xr.Dataset({
        "lat": (["y", "x"], dshrrr.latitude.values),
        "lon": (["y", "x"], dshrrr.longitude.values)})
    
    # Define target lat/lon grid with ~3km spacing (~0.03º)
    lat_min, lat_max = float(input_grid.lat.values.min()), float(input_grid.lat.values.max())
    lon_min, lon_max = float(input_grid.lon.values.min()), float(input_grid.lon.values.max())
    target_lat = np.arange(lat_min, lat_max + 0.03, 0.03)
    target_lon = np.arange(lon_min, lon_max + 0.03, 0.03)
    lon2d, lat2d = np.meshgrid(target_lon, target_lat)
    output_grid= xr.Dataset({
        "lat": (["y", "x"], lat2d),
        "lon": (["y", "x"], lon2d)
    })
    
    # Create and apply regridder
    regridder  = xe.Regridder(input_grid, output_grid, method="bilinear", 
                              periodic=False, reuse_weights=False)
    da_channels_= regridder(dshrrr)
    #
    da_channels_= da_channels_.assign_coords(
        y=("y", output_grid["lat"][:, 0].data),
        x=("x", output_grid["lon"][0, :].data),
        latitude=(("y", "x"), output_grid["lat"].data),
        longitude=(("y", "x"), output_grid["lon"].data)
    )
    da_channels_clean = da_channels_.drop_vars(['metpy_crs', 'latitude','longitude'])
    da_channels_clean = da_channels_clean.rename({'x': 'lon','y': 'lat'})
    
    return da_channels_clean

print('starting lat/lon regridding of HRRR data')
dshrrr_u = regriddata(dshrrr_u)
dshrrr_v = regriddata(dshrrr_v)
dshrrr_ws = regriddata(dshrrr_ws)
dshrrr_rh = regriddata(dshrrr_rh)
dshrrr_tmp = regriddata(dshrrr_tmp)
dshrrr_dpt = regriddata(dshrrr_dpt)
dshrrr_pot = regriddata(dshrrr_pot)
dshrrr_spfh = regriddata(dshrrr_spfh)
print('finished lat/lon regridding of HRRR data')
#####################################################################################################
# Combine data arrays into one dataset

dshrrr = xr.Dataset({
    'u': dshrrr_u['UGRD'],
    'v': dshrrr_v['VGRD'],
    'ws': dshrrr_ws['ws'],
    'rh': dshrrr_rh['RH'],
    'tmp': dshrrr_tmp['TMP'],
    'dpt': dshrrr_dpt['DPT'],
    'pot': dshrrr_pot['POT'],
    'spfh': dshrrr_spfh['SPFH']

})

print('grouped all variables into one dataset')
#####################################################################################################
# Functions for Regridding to Healpix

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

#####################################################################################################
# Get dataset to convert to
print('getting healpix dataset for regridding')
# List available catalogs
catalog_file = "https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml"
# Select a catalog for your location
current_location = "NERSC"
cat = intake.open_catalog(catalog_file)[current_location]

# Get zoom level from catalog
highest_zoom_level = pd.DataFrame(cat["scream2D_hrly"].describe()["user_parameters"]).allowed.max()[0]
catalog_params = {'zoom': highest_zoom_level}  # Can have multiple parameters

# Note the use of **catalog_params to pass the parameters
ds_hp = cat['scream2D_hrly'](**catalog_params).to_dask()

# Make a flag for mask longitude sign
signed_lon = True if np.min(dshrrr.lon) < 0 else False

# Add lat/lon coordinates to the DataSet.
# Set signed_lon=True for matching lat/lon DataSet with longitude -180 to +180
ds_hp = ds_hp.pipe(partial(egh.attach_coords,signed_lon=signed_lon))

# Assign extra coordinates (lon_hp, lat_hp) to the HEALPix coordinates
# This is needed for limiting the extrapolation during remapping
lon_hp = ds_hp.lon.assign_coords(cell=ds_hp.cell, lon_hp=lambda da: da)
lat_hp = ds_hp.lat.assign_coords(cell=ds_hp.cell, lat_hp=lambda da: da)
#####################################################################################################
# Convert to healpix
print('starting HRRR regridding to healpix')
# Remap mask DataSet to HEALPix
tolerance = calculate_healpix_tolerance(highest_zoom_level)
ds_HRRR_hp = dshrrr.pipe(fix_coords).sel(lon=lon_hp, lat=lat_hp, method="nearest").where(partial(is_valid, tolerance=tolerance))
# Drop unnecessary coordinates
ds_HRRR_hp = ds_HRRR_hp.drop_vars(["lat_hp", "lon_hp","lon","lat"])
print('finished HRRR regridding to healpix')
#####################################################################################################
# Functions for saving at different zoom levels as zarr files

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

#####################################################################################################
# Save at different zoom levels as zarr files
print('saving HRRR data to zarr files at different zoom levels')
dn = ds_HRRR_hp
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







