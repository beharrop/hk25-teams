#!/bin/bash

# Script is best run on interactive nodes
# salloc --nodes 1 --qos interactive --time 2:00:00 --constraint cpu --account=$1
# On Perlmutter CPU node, this took roughly 21 minutes to complete 13 months of
# SCREAM data with ne120 (~quarter degree grid spacing)

main() {

echo starting main
    
source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

# This section is separated into multiple parts
# Part 1 is to specify the user choices for directory and file
#   naming conventions (note that Part 3 will take those specifications
#   to build the names.
#   
# 

############################################################
### Part 1 - Case specifiers (change these for each run) ###
############################################################
output_dir="/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/scream_1year_test/"
if [ ! -d "${output_dir}" ]; then
    mkdir -p ${output_dir}
fi

input_file=""
input_list="${output_dir}/scream2D_ne120_input_hp8.txt"

if [ -z "${input_file}" ]; then
    rm -f $input_list
    make_list $input_list /pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/scream_1year_data/scream2D_ne120_inst_ivt_hp8.??????.nc
fi

# A name to attach to intermediate files
shortname="scream2D_ne120_hp8_fast"

# Connectivity file
connectfile="/global/cfs/cdirs/m1867/beharrop/TempestExtremes/hk25_tracking/grids/connect_healpix_grid_zoom_8_format_exodus_corrected_by_scrip.txt"

# CRS metadata doesn't propagate through, so we will use NCO to append it at the end
crs_file="/global/cfs/cdirs/m1867/beharrop/TempestExtremes/hk25_tracking/grids/crs_data_healpix_zoom_8.nc"

# These flags control which features to detect and whether to do file cleanup
do_detect_tc=true
do_detect_ar=true
do_detect_etc=true
do_file_cleanup=true



############################################################
### Part 2 - Specify flags for TC  detection algorithms  ###
############################################################
# Detecting Tropical Cyclones has four steps
#   1 – DetectNodes    - takes input_file & output_file or
#                        input_list & output_list)
#   2 – StitchNodes    - stitches together the detected
#                        nodes from step 1
#   3 – NodeFileFilter - further filters nodes based on user
#                        criteria and generates a binary
#                        mask
#   4 - StitchBlobs    - stitch the nodes together in time
#                        and give each storm a unique
#                        integer tag

# Specify DetectNodes arguments
# 1) Search for candidates as minima in the sea level pressure field
# --searchbymin determines the variable to search for local minima
arg_searchbymin=psl 
# --closedcontourcmd specifies 
arg_closedcontourcmd="psl,200.0,5.5,0;_DIFF(zg300,zg500),-6.0,6.5,1.0" 
# --mergedist
arg_mergeddist=6.0 
# --outputcmd
arg_outputcmd="psl,min,0;sfcWind,max,2;ELEV,min,0;pr,avg,4" 
# --timefilter
arg_timefilter="3hr" 



############################################################
### Part 3 - Specify flags for AR  detection algorithms  ###
############################################################



############################################################
### Part 4 - Specify flags for ETC detection algorithms  ###
############################################################



############################################################
### Part 5 - Construct pathing and file names based on   ###
###          user input and then let's detect things!    ###
############################################################

generate_file_names
detect_tc
detect_ar
detect_etc
file_cleanup

echo Detection script has finished running.  Congratulations!

} # end main

make_list() {

    # Examples:
    # make_list myfile_list.txt /path/to/file1.nc /path/to/file2.nc
    # make_list myfile_list.txt /path/to/file*.nc
   
    # Check if at least two arguments are provided
    if [ "$#" -lt 2 ]; then
	echo "Usage: make_list <output_file> <file1> [file2] ... [fileN]"
	return 1
    fi

    # Extract the first argument as the output file
    local output_file="$1"
    shift  # Shift arguments to access the rest

    # Write remaining arguments (file paths) to the output file
    for file in "$@"; do
	echo "$file" >> "$output_file"
    done
}

modify_list() {
    # Examples
    # modify_list input_list.txt output_list.txt new_path_string name_prefix_string
    # modify_list input_list.txt output_list.txt new_path_string name_prefix_string .txt
    # Ensure at least two basic arguments: input file list and output file list
    if [ "$#" -lt 4 ]; then
        echo "Usage: change_paths_and_names <input_filelist> <output_filelist> <new_path> <name_prefix> [name_suffix]"
        return 1
    fi

    # Assign arguments to variables
    local input_filelist="$1"
    local output_filelist="$2"
    local new_path="$3"
    local name_prefix="$4"

    # Assign optional argument (name_suffix) if provided; default to '.nc'
    local name_suffix="${5:-.nc}"

    # Ensure the input file list exists
    if [[ ! -f $input_filelist ]]; then
        echo "Input file list '$input_filelist' not found!"
        return 1
    fi

    # Create the output file, overwriting if it exists
    > "$output_filelist"

    # Read the input file list and transform each path and filename
    while IFS= read -r file_path; do
        # Extract the basename without directory and extension
        local filename=$(basename "$file_path" .nc)

        # Construct the new path and filename based on the provided new path and prefix
        local new_file_path="${new_path}/${name_prefix}${filename}${name_suffix}"

        # Append the new file path to the output file list
        echo "$new_file_path" >> "$output_filelist"
    done < "$input_filelist"
}

generate_file_names() {
    # TC files
tc_detected_nodes=${output_dir}/${shortname}".tc_detected_nodes.txt"
tc_stitched_nodes=${output_dir}/${shortname}".tc_stitched_nodes.txt"
tc_filtered_nodes_file=${output_dir}/${shortname}".tc_filtered_nodes.nc"
tc_filtered_nodes_list=${output_dir}/${shortname}".tc_filtered_nodes.txt"
tc_tracks_file=${output_dir}/${shortname}".tc_tracks.nc"
tc_tracks_list=${output_dir}/${shortname}".tc_tracks.txt"
tc_climatology=${output_dir}/${shortname}".tc_climatology.nc"

if [ -z "${input_file}" ]; then
    modify_list $input_list $tc_detected_nodes $output_dir TC_det_nodes_ .txt
    modify_list $input_list $tc_filtered_nodes_list $output_dir TC_filt_nodes_
    modify_list $input_list $tc_tracks_list $output_dir TC_tracks_
fi

# AR files
ar_detected_blobs_file=${output_dir}/${shortname}".ar_detected_blobs.nc"
ar_detected_blobs_list=${output_dir}/${shortname}".ar_detected_blobs.txt"
ar_filtered_nodes_file=${output_dir}/${shortname}".ar_filtered_nodes.nc"
ar_filtered_nodes_list=${output_dir}/${shortname}".ar_filtered_nodes.txt"
ar_tracks_file=${output_dir}/${shortname}".ar_tracks.nc"
ar_tracks_list=${output_dir}/${shortname}".ar_tracks.txt"

if [ -z "${input_file}" ]; then
    modify_list $input_list $ar_detected_blobs_list $output_dir AR_det_blobs_
    modify_list $input_list $ar_filtered_nodes_list $output_dir AR_filt_nodes_
    modify_list $input_list $ar_tracks_list $output_dir AR_tracks_
fi

# ETC files
etc_detected_nodes=${output_dir}/${shortname}".etc_detected_nodes.txt"
etc_stitched_nodes=${output_dir}/${shortname}".etc_stitched_nodes.txt"
etc_filtered_nodes_file=${output_dir}/${shortname}".etc_filtered_nodes.nc"
etc_filtered_nodes_list=${output_dir}/${shortname}".etc_filtered_nodes.txt"
etc_tracks_file=${output_dir}/${shortname}".etc_tracks.nc"
etc_tracks_list=${output_dir}/${shortname}".etc_tracks.txt"

if [ -z "${input_file}" ]; then
    modify_list $input_list $etc_detected_nodes $output_dir ETC_det_nodes_ .txt
    modify_list $input_list $etc_filtered_nodes_list $output_dir ETC_filt_nodes_
    modify_list $input_list $etc_tracks_list $output_dir ETC_tracks_
fi
}

detect_tc() {

if [ "${do_detect_tc,,}" != "true" ]; then
    echo $'\n----- Skipping TC Detection -----\n'
    return
fi

# Clear out the TC files from previous attempts when using just a file
rm -f ${tc_stitched_nodes}
if [ -z "${input_list}" ]; then
    rm -f ${tc_detected_nodes}
    rm -f ${tc_filtered_nodes_file}
    rm -f ${tc_tracks_file}
    rm -f ${tc_climatology}
fi


### Go into bulk of the code (X steps)
# 1 – DetectNodes (takes input_file & output_file or input_list & output_list)
# 2 – StitchNodes
# 3 – NodeFileFilter
# 4 – Climatology

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
if [ -z "${input_list}" ]; then
    srun -n 64 DetectNodes \
         --in_data ${input_file} \
	 --out ${tc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0;_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 6.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;ELEV,min,0;pr,avg,4" \
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
	--in_fmt "lon,lat,slp,wind,zs,pr" \
	--range 8.0 --mintime "10" --maxgap "6" \
	--threshold "wind,>=,10.0,20;lat,<=,50.0,20;lat,>=,-50.0,20;zs,<=,15.0,20" \
	--in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    StitchNodes \
	--in ${tc_detected_nodes} \
	--out ${tc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs,pr" \
	--range 8.0 --mintime "10" --maxgap "6" \
	--threshold "wind,>=,10.0,20;lat,<=,50.0,20;lat,>=,-50.0,20;zs,<=,15.0,20" \
	--in_connect ${connectfile}
fi

### NodeFileFilter (Step 3)
#   1) Unmask regions within 5 degrees of each node
if [ -z "${input_file}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
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
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
	 --in_data ${input_file} \
	 --out_data ${tc_filtered_nodes_file} \
	 --bydist 5.0 \
	 --maskvar "TC_binary_tag" \
	 --var "pr" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

### StitchBlobs (Step 4)
#   1) Require the event to last at least 1 day
#   2) Require 20% of the "blob" to overlap from time step to time step
if [ -z "${input_file}" ]; then
    srun -n 1 StitchBlobs \
	 --in_list ${tc_filtered_nodes_list} \
	 --out_list ${tc_tracks_list} \
	 --var "TC_binary_tag" \
	 --outvar "TC_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then

    srun -n 1 StitchBlobs \
	 --in ${tc_filtered_nodes_file} \
	 --out ${tc_tracks_file} \
	 --var "TC_binary_tag" \
	 --outvar "TC_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi

### Climatology (Step 5)
#   1)
#srun -n 32 Climatology \
#     --in_data_list SCTL_TC_files_PRECT_OUT.txt \
#     --var "precip_total_surf_mass_flux" \
#     --period "annual" --memmax "8G" --verbose --temp_file_path "./tmp" \
#     --out_data SCTL.accumulated_tp_3h.tc_climatology.nc



} # end detect_tc()

detect_ar() {

if [ "${do_detect_ar,,}" != "true" ]; then
    echo $'\n----- Skipping AR Detection -----\n'
    return
fi

if [ -z "${input_list}" ]; then
    rm -f ${ar_detected_blobs_file}
    rm -f ${ar_filtered_nodes_file}
    rm -f ${ar_tracks_file}
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
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
	 --in_data_list ${ar_detected_blobs_list} \
	 --out_data_list ${ar_filtered_nodes_list} \
	 --bydist 8.0 --invert --var "AR_binary_tag" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${tc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
	 --in_data ${ar_detected_blobs_file} \
	 --out_data ${ar_filtered_nodes_file} \
	 --bydist 8.0 --invert --var "AR_binary_tag" \
	 --in_connect ${connectfile}
fi


### StitchBlobs (Step 3)
#   1) Require the event to last at least 1 day
#   2) Require 20% of the "blob" to overlap from time step to time step
if [ -z "${input_file}" ]; then
    srun -n 1 StitchBlobs \
	 --in_list ${ar_filtered_nodes_list} \
	 --out_list ${ar_tracks_list} \
	 --var "AR_binary_tag" \
	 --outvar "AR_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then

    srun -n 1 StitchBlobs \
	 --in ${ar_filtered_nodes_file} \
	 --out ${ar_tracks_file} \
	 --var "AR_binary_tag" \
	 --outvar "AR_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi

} # end detect_ar()


detect_etc() {

if [ "${do_detect_etc,,}" != "true" ]; then
    echo $'\n----- Skipping ETC Detection -----\n'
    return
fi

rm -f ${etc_stitched_nodes}
if [ -z "${input_list}" ]; then
    rm -f ${etc_detected_nodes}
    rm -f ${etc_filtered_nodes_file}
    rm -f ${etc_tracks_file}
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
if [ -z "${input_list}" ]; then
    srun -n 64 DetectNodes \
	 --in_data ${input_file} \
	 --out ${etc_detected_nodes} \
	 --searchbymin psl \
	 --closedcontourcmd "psl,200.0,5.5,0" \
	 --noclosedcontourcmd "_DIFF(zg300,zg500),-6.0,6.5,1.0" \
	 --mergedist 9.0 \
	 --outputcmd "psl,min,0;sfcWind,max,2;ELEV,min,0;pr,avg,4" \
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
	--in_fmt "lon,lat,slp,wind,zs,pr" \
	--range 9.0 --mintime "24h" --maxgap "2" \
	--min_endpoint_dist 12.0 \
	--in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    StitchNodes  \
	--in ${etc_detected_nodes} \
	--out ${etc_stitched_nodes} \
	--in_fmt "lon,lat,slp,wind,zs,pr" \
	--range 9.0 --mintime "24h" --maxgap "2" \
	--min_endpoint_dist 12.0 \
	--in_connect ${connectfile}
fi

### NodeFileFilter (Step 3)
#   1) Unmask regions within 10 degrees of each node
if [ -z "${input_file}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${etc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
	 --in_data_list ${input_list} \
	 --out_data_list ${etc_filtered_nodes_list} \
	 --bydist 10.0 \
	 --maskvar "ETC_binary_tag" \
	 --var "pr" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then
    srun -n 32 NodeFileFilter \
	 --in_nodefile ${etc_stitched_nodes} \
	 --in_fmt "lon,lat,psl,umax,zs,pr" \
	 --in_data ${input_file} \
	 --out_data ${etc_filtered_nodes_file} \
	 --bydist 10.0 \
	 --maskvar "ETC_binary_tag" \
	 --var "pr" \
	 --timefilter "3hr" \
	 --in_connect ${connectfile}
fi

### StitchBlobs (Step 4)
#   1) Require the event to last at least 1 day
#   2) Require 20% of the "blob" to overlap from time step to time step
if [ -z "${input_file}" ]; then
    srun -n 1 StitchBlobs \
	 --in_list ${etc_filtered_nodes_list} \
	 --out_list ${etc_tracks_list} \
	 --var "ETC_binary_tag" \
	 --outvar "ETC_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi
if [ -z "${input_list}" ]; then

    srun -n 1 StitchBlobs \
	 --in ${etc_filtered_nodes_file} \
	 --out ${etc_tracks_file} \
	 --var "ETC_binary_tag" \
	 --outvar "ETC_count_index" \
	 --mintime "1d" --min_overlap_prev 20 \
	 --in_connect ${connectfile}
fi

} # end detect_etc()

file_cleanup() {

    if [ "${do_file_cleanup,,}" != "true" ]; then
	echo $'\n----- Skipping File Cleanup -----\n'
	return
    fi

    if [ "${do_detect_tc,,}" == "true" ]; then
	echo Cleaning up TC files... 
	# Open up file permissions, append CRS data, and rename ncol to cell
	chmod 644 ${tc_detected_nodes}
	chmod 644 ${tc_stitched_nodes}
	if [ -z "${input_list}" ]; then
	    ncks -A ${crs_file} ${tc_filtered_nodes_file}
	    ncks -A ${crs_file} ${tc_tracks_file}
	    python unify_dimensions.py --input_file ${tc_filtered_nodes_file}
	    python unify_dimensions.py --input_file ${tc_tracks_file}
	    chmod 644 ${tc_filtered_nodes_file}
	    chmod 644 ${tc_tracks_file}
	fi
	if [ -z "${input_file}" ]; then
	    chmod 644 ${tc_filtered_nodes_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${tc_filtered_nodes_list}"
	    chmod 644 ${tc_tracks_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${tc_tracks_list}"
	fi
	echo TC files cleaned up
    fi

    if [ "${do_detect_ar,,}" == "true" ]; then
	echo Cleaning up AR files... 
	# Open up file permissions, append CRS data, and rename ncol to cell
	if [ -z "${input_list}" ]; then
	    ncks -A ${crs_file} ${ar_detected_blobs_file}
	    ncks -A ${crs_file} ${ar_filtered_nodes_file}
	    ncks -A ${crs_file} ${ar_tracks_file}
	    python unify_dimensions.py --input_file ${ar_detected_blobs_file} 
	    python unify_dimensions.py --input_file ${ar_filtered_nodes_file} 
	    python unify_dimensions.py --input_file ${ar_tracks_file}
	    chmod 644 ${ar_detected_blobs_file}
	    chmod 644 ${ar_filtered_nodes_file}
	    chmod 644 ${ar_tracks_file}
	fi
	if [ -z "${input_file}" ]; then
	    chmod 644 ${ar_detected_blobs_list}
	    chmod 644 ${ar_filtered_nodes_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${ar_detected_blobs_list}"
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${ar_filtered_nodes_list}"
	    chmod 644 ${ar_tracks_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${ar_tracks_list}"
	fi
	echo AR files cleaned up
    fi

    if [ "${do_detect_etc,,}" == "true" ]; then
	echo Cleaning up ETC files... 
	# Open up file permissions, append CRS data, and rename ncol to cell
	chmod 644 ${etc_detected_nodes}
	chmod 644 ${etc_stitched_nodes}
	if [ -z "${input_list}" ]; then
	    ncks -A ${crs_file} ${etc_filtered_nodes_file}
	    ncks -A ${crs_file} ${etc_tracks_file}
	    python unify_dimensions.py --input_file ${etc_filtered_nodes_file}
	    python unify_dimensions.py --input_file ${etc_tracks_file}
	    chmod 644 ${etc_filtered_nodes_file}
	    chmod 644 ${etc_tracks_file}
	fi
	if [ -z "${input_file}" ]; then
	    chmod 644 ${etc_filtered_nodes_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${etc_filtered_nodes_list}"
	    chmod 644 ${etc_tracks_list}
	    while IFS= read -r file_name; do
		ncks -A ${crs_file} "$file_name"
		python unify_dimensions.py --input_file "$file_name"
		chmod 644 "$file_name"
	    done < "${etc_tracks_list}"
	fi
	echo ETC files cleaned up
    fi
    
}

# Run the script
main

