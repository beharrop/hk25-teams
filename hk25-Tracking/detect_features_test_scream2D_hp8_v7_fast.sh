#!/bin/bash

# Script is best run on interactive nodes
# salloc --nodes 1 --qos interactive --time 2:00:00 --constraint cpu --account=$1

main() {

echo starting main
    
source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

echo setting up stuff
### Case specifiers (change these for each run)
output_dir="/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/"

input_file="/pscratch/sd/w/wcmca1/scream-cess-healpix/scream2D_ne120_inst_all_hp8_v7_fast.nc"
input_list=""

# A name to attach to intermediate files
shortname="scream2D_ne120_hp8_fast"

# TC files
tc_detected_nodes=${output_dir}/${shortname}".tc_detected_nodes.txt"
tc_stitched_nodes=${output_dir}/${shortname}".tc_stitched_nodes.txt"
tc_filtered_nodes_file=${output_dir}/${shortname}".tc_filtered_nodes.nc"
tc_filtered_nodes_list=${output_dir}/${shortname}".tc_filtered_nodes.txt"
tc_climatology=${output_dir}/${shortname}".tc_climatology.nc"

# AR files
ar_detected_blobs_file=${output_dir}/${shortname}".ar_detected_blobs.nc"
ar_detected_blobs_list=${output_dir}/${shortname}".ar_detected_blobs.txt"
ar_filtered_nodes_file=${output_dir}/${shortname}".ar_filtered_nodes.nc"
ar_filtered_nodes_list=${output_dir}/${shortname}".ar_filtered_nodes.txt"

# ETC files
etc_detected_nodes=${output_dir}/${shortname}".etc_detected_nodes.txt"
etc_stitched_nodes=${output_dir}/${shortname}".etc_stitched_nodes.txt"
etc_filtered_nodes_file=${output_dir}/${shortname}".etc_filtered_nodes.nc"
etc_filtered_nodes_list=${output_dir}/${shortname}".etc_filtered_nodes.txt"

# Connectivity file
connectfile="/global/cfs/cdirs/m1867/beharrop/TempestExtremes/hk25_tracking/grids/connect_healpix_grid_zoom_8_format_exodus_corrected_by_scrip.txt"

do_detect_tc=false
do_detect_ar=false
do_detect_etc=true

# Let's detect things!
detect_tc
detect_ar
detect_etc

} # end main

detect_tc() {

if [ "${do_detect_tc,,}" != "true" ]; then
    echo $'\n----- Skipping TC Detection -----\n'
    return
fi

# Clear out the TC files from previous attempts
rm -f ${tc_detected_nodes}
rm -f ${tc_stitched_nodes}
if [ -z "${input_list}" ]; then
    rm -f ${tc_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    rm -f ${tc_filtered_nodes_list}
fi
rm -f ${tc_climatology}

### Go into bulk of the code ( steps)
# 1 – DetectNodes (takes input_file & output_file or input_list & output_list)
# 2 – StitchNodes
# 3 – NodeFileFilter
# 4 – Climatology
# 5 - Prepare files for ZARR conversion (append CRS and change dimension name to cell)

### Detect tropical cyclones (Step 1)
#   1) Search for candidates as minima in the sea level pressure field
#   2) Merge candidates within 6 degrees
#   3) Enforce a closed contour increase of 200 Pa within 5.5 degrees of candidate
#   4) Enforce a closed contour decrease of 6 m contour within 1 degree of the maximum
#      value of thickness between 300 hPa and 500 hPa
#   5) Output sea level pressure minima at the candidate point, wind speed 
#      maximum, and average precipitation (convective + large-scale) within 
#      4 degrees of candidate

if [ -z "${input_file}" ]; then
    srun -n 64 DetectNodes \
         --in_data_list ${input_list} \
	 --out_file_list ${tc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0;_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 6.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;ELEV,min,0;pr,avg,4" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi
echo "WARNING THAT THE ELEVATION CRITERION IS NOT IMPLEMENTED HERE"
if [ -z "${input_list}" ]; then
    srun -n 64 DetectNodes \
         --in_data ${input_file} \
	 --out ${tc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0;_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 6.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;sfcWind,min,2" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

### Stitch nodes (Step 2)
#   1) With a maximum distance of 8.0 degrees between in-sequence candidates
#   2) With a minimum length of 10 hours
#   3) With a maximum gap size of 6 time points (18 hours)
#   4) With a minimum windspeed of 10.0 for 20 time points (60 hours)
#   5) With a location between 50S and 50N for 20 time points (60 hours)
#   6) With surface elevation below 15 m for 20 time points (60 hours)

if [ -z "${input_file}" ]; then
    StitchNodes \
	--in_list ${tc_detected_nodes} \
	--out ${tc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs" \
	--range 8.0 --mintime "10" --maxgap "6" \
	--threshold "wind,>=,10.0,20;lat,<=,50.0,20;lat,>=,-50.0,20;zs,<=,15.0,20" \
	--in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    StitchNodes \
	--in ${tc_detected_nodes} \
	--out ${tc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs" \
	--range 8.0 --mintime "10" --maxgap "6" \
	--threshold "wind,>=,10.0,20;lat,<=,50.0,20;lat,>=,-50.0,20;zs,<=,15.0,20" \
	--in_connect ${connectfile}
fi

### NodeFileFilter (Step 3)
#   1) Unmask regions within 5 degrees of each node
if [ -z "${input_file}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data_list ${input_list} \
	 --out_data_list ${tc_filtered_nodes_list} \
	 --bydist 5.0 \
	 --maskvar "TC_binary_tag" \
	 --var "pr" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data ${input_file} \
	 --out_data ${tc_filtered_nodes_file} \
	 --bydist 5.0 \
	 --maskvar "TC_binary_tag" \
	 --var "pr" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

### Climatology (Step 4)
#   1)
#srun -n 32 Climatology \
#     --in_data_list SCTL_TC_files_PRECT_OUT.txt \
#     --var "precip_total_surf_mass_flux" \
#     --period "annual" --memmax "8G" --verbose --temp_file_path "./tmp" \
#     --out_data SCTL.accumulated_tp_3h.tc_climatology.nc

chmod 644 ${tc_detected_nodes}
chmod 644 ${tc_stitched_nodes}
if [ -z "${input_list}" ]; then
    chmod 644 ${tc_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    chmod 644 ${tc_filtered_nodes_list}
fi
#chmod 644 ${tc_detected_nodes}

} # end detect_tc()

detect_ar() {

if [ "${do_detect_ar,,}" != "true" ]; then
    echo $'\n----- Skipping AR Detection -----\n'
    return
fi

if [ -z "${input_list}" ]; then
    rm -f ${ar_detected_blobs_file}
    rm -f ${ar_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    rm -f ${ar_detected_blobs_list}
    rm -f ${ar_filtered_nodes_list}
fi

### DetectBlobs (Step 1)
#   1) Threshold points where the Laplacian of IVT is less than or equal
#      to -30000 kg/m2/s/rad2 (using 8 radial points at a 10 degree great
#      circle distance)
#   2) Filter out points where the geographical area does not exceed
#      850000 square km
if [ -z "${input_file}" ]; then
    srun -n 32 DetectBlobs \
	 --in_data_list ${input_list} \
	 --out_list ${ar_detected_blobs_list} \
	 --thresholdcmd "_LAPLACIAN{8,10.0}(_VECMAG(uivt,vivt)),<=,-30000,0" \
	 --geofiltercmd "area,>,850000km2" \
	 --timefilter "3hr" \
	 --tagvar "AR_binary_tag" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 DetectBlobs \
	 --in_data ${input_file} \
	 --out ${ar_detected_blobs_file} \
	 --thresholdcmd "_LAPLACIAN{8,10.0}(_VECMAG(uivt,vivt)),<=,-30000,0" \
	 --geofiltercmd "area,>,850000km2" \
	 --timefilter "3hr" \
	 --tagvar "AR_binary_tag" \
	 --in_connect ${connectfile}
fi


### NodeFileFilter (Step 2)
#   1) Unmask regions within 8 degrees of each node
#   2) Invert the generated mask (get rid of the TC nodes)
if [ -z "${input_file}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data_list ${ar_detected_blobs_list} \
	 --out_data_list ${ar_filtered_nodes_list} \
	 --bydist 8.0 --invert --var "AR_binary_tag" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data ${ar_detected_blobs_file} \
	 --out_data ${ar_filtered_nodes_file} \
	 --bydist 8.0 --invert --var "AR_binary_tag" \
	 --in_connect ${connectfile}
fi

if [ -z "${input_list}" ]; then
    chmod 644 ${ar_detected_blobs_file}
    chmod 644 ${ar_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    chmod 644 ${ar_detected_blobs_list}
    chmod 644 ${ar_filtered_nodes_list}
fi

} # end detect_ar()


detect_etc() {

if [ "${do_detect_etc,,}" != "true" ]; then
    echo $'\n----- Skipping ETC Detection -----\n'
    return
fi

rm -f ${etc_detected_nodes}
rm -f ${etc_stitched_nodes}
if [ -z "${input_list}" ]; then
    rm -f ${etc_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    rm -f ${etc_filtered_nodes_list}
fi

### Detect extratropical cyclones (Step 1)
#   1) Search for candidates as minima in the sea level pressure field
#   2) Merge candidates within 6 degrees
#   3) Enforce a closed contour increase of 200 Pa within 5.5 degrees of candidate
#   4) Enforce no closed contour decrease of 6 m contour within 1 degree of the maximum
#      value of thickness between 300 hPa and 500 hPa (opposite to TCs)
#   5) Output sea level pressure minima at the candidate point, wind speed 
#      maximum, and average precipitation (convective + large-scale) within 
#      4 degrees of candidate
if [ -z "${input_file}" ]; then
    srun -n 64 DetectNodes \
	 --in_data_list ${input_list} \
	 --out_file_list ${etc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0" \
	 --noclosedcontourcmd "_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 9.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;ELEV,min,0;pr,avg,4" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi
echo "WARNING THAT THE ELEVATION CRITERION IS NOT IMPLEMENTED HERE"
if [ -z "${input_list}" ]; then
    srun -n 64 DetectNodes \
	 --in_data ${input_file} \
	 --out ${etc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0" \
	 --noclosedcontourcmd "_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 9.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;sfcWind,min,2" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

### Stitch nodes (Step 2)
#   1) With a maximum distance of 9.0 degrees between in-sequence candidates
#   2) With a minimum length of 24 hours
#   3) With a maximum gap size of 2 time points (6 hours)
#   4) With a minimum great-circle distance between the start and end points of 12
if [ -z "${input_file}" ]; then
    StitchNodes  \
	--in_list ${etc_detected_nodes} \
	--out ${etc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs" \
	--range 9.0 --mintime "24h" --maxgap "2" \
	--min_endpoint_dist 12.0 \
	--in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    StitchNodes  \
	--in ${etc_detected_nodes} \
	--out ${etc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs" \
	--range 9.0 --mintime "24h" --maxgap "2" \
	--min_endpoint_dist 12.0 \
	--in_connect ${connectfile}
fi

### NodeFileFilter (Step 3)
#   1) Unmask regions within 10 degrees of each node
if [ -z "${input_file}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${etc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data_list ${input_list} \
	 --out_data_list ${etc_filtered_nodes_list} \
	 --bydist 10.0 \
	 --maskvar "ETC_binary_tag" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${etc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs" \
	 --in_data ${input_file} \
	 --out_data ${etc_filtered_nodes_file} \
	 --bydist 10.0 \
	 --maskvar "ETC_binary_tag" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

chmod 644 ${etc_detected_nodes}
chmod 644 ${etc_stitched_nodes}
if [ -z "${input_list}" ]; then
    chmod 644 ${etc_filtered_nodes_file}
fi
if [ -z "${input_file}" ]; then
    chmod 644 ${etc_filtered_nodes_list}
fi

} # end detect_etc()

# Run the script
main

