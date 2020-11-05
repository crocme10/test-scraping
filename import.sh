#!/usr/bin/env bash

set -o errexit
set -o nounset

readonly SCRIPT_SRC="$(dirname "${BASH_SOURCE[${#BASH_SOURCE[@]} - 1]}")"
readonly SCRIPT_DIR="$(cd "${SCRIPT_SRC}" >/dev/null 2>&1 && pwd)"
readonly SCRIPT_NAME=$(basename "$0")

SETTINGS_FILE="settings.json" # default, can be set by command line.
APPLICATION="${SCRIPT_NAME%.*}"
VERSION=0.0.1
EXECUTION_DATE=`date '+%Y%m%d'`
LOG_FILE="${APPLICATION}-${EXECUTION_DATE}.log"
CONFIG_FILE="${APPLICATION}.rc"
QUIET=false
VERBOSE=false



version()
{
  echo ""
  echo "${APPLICATION}-${VERSION}"
  echo ""
}

usage()
{
  echo ""
  echo "${APPLICATION} - Download data and import into Elasticsearch"
  echo ""
  echo "This file is configured with ${CONFIG_FILE}."
  echo ""
  echo "${APPLICATION} "
  echo "  [ -c ]                Config file"
  echo "  [ -s ]                Settings file"
  echo "  [ -v ]                Displays version information"
  echo "  [ -V ]                Verbose"
  echo "  [ -q ]                Quiet, doesn't display to stdout or stderr"
  echo "  [ -h ]                Displays this message"
  echo ""
}

# http://stackoverflow.com/questions/369758/how-to-trim-whitespace-from-bash-variable
trim()
{
  local var=$1
  var="${var#"${var%%[![:space:]]*}"}"   # remove leading whitespace characters
  var="${var%"${var##*[![:space:]]}"}"   # remove trailing whitespace characters
  echo -n "$var"
}

# $1: info message string
log_debug()
{
    DATE=`date -R`
    if [ "${QUIET}" = false ] && [ "${VERBOSE}" = true ]; then
        echo -e "\e[90m$DATE | $1\e[0m"
        echo "DEBUG | $DATE | $1" >> $LOG_FILE
    fi
}

# $1: info message string
log_info()
{
    DATE=`date -R`
    if [ "${QUIET} = false" ]; then
        echo -e "\e[90m$DATE | $1\e[0m"
    fi
    echo "INFO  | $DATE | $1" >> $LOG_FILE
}

# $1: error message string
log_error()
{
    DATE=`date -R`
    if [ "${QUIET}" = false ]; then
        echo -e "\e[91m$DATE | $1\e[0m" >&2
    fi
    echo "ERROR | $DATE | $1" >> $LOG_FILE
}

# We check all the executables that will be called in this script.
check_requirements()
{
    log_info "Checking requirements"

    log_debug "Checking curl"
    command -v curl > /dev/null 2>&1  || { log_error "curl not found. You need to install curl"; return 1; }
    log_debug "Checking docker"
    command -v docker > /dev/null 2>&1  || { log_error "docker not found. You need to install docker"; return 1; }
    log_debug "Checking jq"
    command -v jq > /dev/null 2>&1  || { log_error "jq not found. You need to install jq"; return 1; }
    log_debug "Checking uuidgen"
    command -v uuidgen > /dev/null 2>&1  || { log_error "uuidgen not found. You need to install uuidgen"; return 1; }

    return 0
}

# We check the validity of the command line arguments and the configuration
check_arguments()
{
    log_info "Checking arguments"
    # Check that the variable $ES_PORT is set and non-empty
    [[ -z "${ES_PORT+xxx}" ]] &&
    { log_error "The variable \$ES_PORT is not set. Make sure it is set in the configuration file."; usage; return 1; }
    [[ -z "$ES_PORT" && "${ES_PORT+xxx}" = "xxx" ]] &&
    { log_error "The variable \$ES_PORT is set but empty. Make sure it is set in the configuration file."; usage; return 1; }

    # Check that the variable $ES_IMAGE is set and non-empty
    [[ -z "${ES_IMAGE+xxx}" ]] &&
    { log_error "The variable \$ES_IMAGE is not set. Make sure it is set in the configuration file."; usage; return 1; }
    [[ -z "$ES_IMAGE" && "${ES_IMAGE+xxx}" = "xxx" ]] &&
    { log_error "The variable \$ES_IMAGE is set but empty. Make sure it is set in the configuration file."; usage; return 1; }

    # Check that the variable $ES_NAME is set and non-empty
    [[ -z "${ES_NAME+xxx}" ]] &&
    { log_error "The variable \$ES_NAME is not set. Make sure it is set in the configuration file."; usage; return 1; }
    [[ -z "$ES_NAME" && "${ES_NAME+xxx}" = "xxx" ]] &&
    { log_error "The variable \$ES_NAME is set but empty. Make sure it is set in the configuration file."; usage; return 1; }

    return 0
}

# We check the presence of directories (possibly create them), and remote machines.
check_environment()
{
    log_info "Checking environment"

    # TODO Check sysctl values for virtual memory

    return 0
}

# $1: string to search for
# $2: a space delimited list of string
# Returns 1 if $1 was found in $2, 0 otherwise
search_in()
{
  KEY="${1}"
  LIST="${2}"
  OIFS=$IFS
  IFS=" "
  for ELEMENT in ${LIST}
  do
    [[ "${KEY}" = "${ELEMENT}" ]] && { return 1; }
  done
  IFS=$OIFS
  return 0
}

# This is a violent function... It tears the existing docker named ${ES_NAME}
start_docker() {
  log_debug "Checking docker ${ES_NAME}"
  local DOCKER_NAMES=`docker ps --all --format '{{.Names}}'`
  if [[ ${DOCKER_NAMES} =~ ${ES_NAME} ]]; then
    log_debug "docker container "${ES_NAME}" is running"
    docker stop ${ES_NAME} > /dev/null 2> /dev/null
    log_debug "docker container "${ES_NAME}" stopped"
    docker rm ${ES_NAME} > /dev/null 2> /dev/null
    log_debug "docker container "${ES_NAME}" removed"
  fi
  log_info "Starting docker container: ${ES_NAME}"
  local DOCKER_CMD="docker run --detach --name ${ES_NAME} --publish ${ES_PORT}:${ES_PORT} --env \"discovery.type=single-node\" ${ES_IMAGE}"
  log_debug "${DOCKER_CMD}"
  local RESP=$(eval ${DOCKER_CMD})
  [[ $? != 0 ]] && { log_error "Could not restart docker '${ES_NAME}' based on settings ($RESP)"; return 1; }
  log_debug "Waiting 10 seconds for Elasticsearch to be up"
  sleep 10
  return 0
}

create_index() {
  local ES_ENDPOINT="http://localhost:${ES_PORT}"
  local CURL_CMD="curl -X PUT '${ES_ENDPOINT}/${ES_INDEX}' -H 'Content-Type: application/json' --data-binary @settings.json"
  log_debug "${CURL_CMD}"
  local RESP=$(eval "${CURL_CMD}")
  [[ $? != 0 ]] && { log_error "Could not create Elasticsearch index '${ES_INDEX}': ($RESP)"; return 1; }
  return 0
}

create_mapping() {
  local ES_ENDPOINT="http://localhost:${ES_PORT}"
  local CURL_CMD="curl -X PUT '${ES_ENDPOINT}/${ES_INDEX}' -H 'Content-Type: application/json' --data-binary @mappings.json"
  log_debug "${CURL_CMD}"
  local RESP=$(eval "${CURL_CMD}")
  [[ $? != 0 ]] && { log_error "Could not create Elasticsearch mapping for '${ES_INDEX}': ($RESP)"; return 1; }
  return 0
}

generate_input_stage_1() {
  return 0
}

generate_input_stage_2() {

  trap 'rm -f "$TMP_INPUT_FILE"' EXIT

  local INPUT_JSON="characters.json"
  TMP_INPUT_FILE=$(mktemp)
  [[ $? != 0 ]] && { log_error "Could not create temporary input file"; return 1; }
  log_debug "Saving bulk input file to ${TMP_INPUT_FILE}"
  OIFS=$IFS
  IFS=$'\n'
  for row in $(cat "${INPUT_JSON}" | jq -c '.[]'); do
    id=$(uuidgen)
    echo "{\"index\": {\"_index\": \"${ES_INDEX}\", \"_type\": \"_doc\", \"_id\": \"${id}\" }}" >> ${TMP_INPUT_FILE}
    echo "${row}" >> ${TMP_INPUT_FILE}
  done
  IFS=$OIFS
  return 0
}

import_data() {
  local ES_ENDPOINT="http://localhost:${ES_PORT}"
  local CURL_CMD="curl -X PUT '${ES_ENDPOINT}/${ES_INDEX}/_doc/_bulk' -H 'Content-Type: application/json' --data-binary @${TMP_INPUT_FILE}"
  log_debug "${CURL_CMD}"
  local RESP=$(eval "${CURL_CMD}")
  [[ $? != 0 ]] && { log_error "Could not import data into Elasticsearch for '${ES_INDEX}': ($RESP)"; return 1; }
  return 0
}

# Pre requisite: DATA_DIR exists.
# generate_cosmogony() {
#   log_info "Generating cosmogony"
#   local COSMOGONY="${COSMO_DIR}/target/release/cosmogony"
#   mkdir -p "$DATA_DIR/cosmogony"
#   command -v "${COSMOGONY}" > /dev/null 2>&1  || { log_error "cosmogony not found in ${COSMO_DIR}. Aborting"; return 1; }
#   local INPUT="${DATA_DIR}/osm/${OSM_REGION}-latest.osm.pbf"
#   local OUTPUT="${DATA_DIR}/cosmogony/${OSM_REGION}.json.gz"
#   [[ -f "${INPUT}" ]] || { log_error "cosmogony cannot run: Missing input ${INPUT}"; return 1; }
#   "${COSMOGONY}" --country-code FR --input "${INPUT}" --output "${OUTPUT}" > /dev/null 2> /dev/null
#   [[ $? != 0 ]] && { log_error "Could not generate cosmogony data for ${OSM_REGION}. Aborting"; return 1; }
#   return 0
# }
# 
# # Pre requisite: DATA_DIR exists.
# import_cosmogony() {
#   log_info "Importing cosmogony into mimir"
#   local COSMOGONY2MIMIR="${MIMIR_DIR}/target/release/cosmogony2mimir"
#   command -v "${COSMOGONY2MIMIR}" > /dev/null 2>&1  || { log_error "cosmogony2mimir not found in ${MIMIR_DIR}. Aborting"; return 1; }
#   local INPUT="${DATA_DIR}/cosmogony/${OSM_REGION}.json.gz"
#   [[ -f "${INPUT}" ]] || { log_error "cosmogony2mimir cannot run: Missing input ${INPUT}"; return 1; }
# 
#   "${COSMOGONY2MIMIR}" --connection-string "http://localhost:${ES_PORT}/${ES_INDEX}" --input "${INPUT}" > /dev/null 2> /dev/null
#   [[ $? != 0 ]] && { log_error "Could not import cosmogony data from ${DATA_DIR}/cosmogony/${OSM_REGION}.json.gz into mimir. Aborting"; return 1; }
#   return 0
# }
# 
# # Pre requisite: DATA_DIR exists.
# import_osm() {
#   log_info "Importing osm into mimir"
#   local OSM2MIMIR="${MIMIR_DIR}/target/release/osm2mimir"
#   command -v "${OSM2MIMIR}" > /dev/null 2>&1  || { log_error "osm2mimir not found in ${MIMIR_DIR}. Aborting"; return 1; }
#   local INPUT="${DATA_DIR}/osm/${OSM_REGION}-latest.osm.pbf"
#   [[ -f "${INPUT}" ]] || { log_error "osm2mimir cannot run: Missing input ${INPUT}"; return 1; }
# 
#   "${OSM2MIMIR}" --import-way --import-poi --input "${DATA_DIR}/osm/${OSM_REGION}-latest.osm.pbf" --config-dir "${SCRIPT_DIR}/../config" -c "http://localhost:${ES_PORT}/${ES_INDEX}" > /dev/null 2> /dev/null
#   [[ $? != 0 ]] && { log_error "Could not import OSM PBF data for ${OSM_REGION} into mimir. Aborting"; return 1; }
#   return 0
# }
# 
# # Pre requisite: DATA_DIR exists.
# download_osm() {
#   log_info "Downloading osm into mimir for ${OSM_REGION}"
#   mkdir -p "$DATA_DIR/osm"
#   wget --quiet --directory-prefix="${DATA_DIR}/osm" "https://download.geofabrik.de/europe/france/${OSM_REGION}-latest.osm.pbf"
#   [[ $? != 0 ]] && { log_error "Could not download OSM PBF data for ${OSM_REGION}. Aborting"; return 1; }
#   return 0
# }
# 
# # Pre requisite: DATA_DIR exists.
# import_ntfs() {
#   log_info "Importing ntfs into mimir"
#   local NTFS2MIMIR="${MIMIR_DIR}/target/release/ntfs2mimir"
#   command -v "${NTFS2MIMIR}" > /dev/null 2>&1  || { log_error "osm2mimir not found in ${MIMIR_DIR}. Aborting"; return 1; }
#   "${NTFS2MIMIR}" --input "${DATA_DIR}/ntfs" -c "http://localhost:${ES_PORT}/${ES_INDEX}" > /dev/null 2> /dev/null
#   [[ $? != 0 ]] && { log_error "Could not import NTFS data from ${DATA_DIR}/ntfs into mimir. Aborting"; return 1; }
#   return 0
# }
# 
# # Pre requisite: DATA_DIR exists.
# download_ntfs() {
#   log_info "Downloading ntfs for ${NTFS_REGION}"
#   mkdir -p "$DATA_DIR/ntfs"
#   wget --quiet -O "${DATA_DIR}/${NTFS_REGION}.csv" "https://navitia.opendatasoft.com/explore/dataset/${NTFS_REGION}/download/?format=csv"
#   [[ $? != 0 ]] && { log_error "Could not download NTFS CSV data for ${NTFS_REGION}. Aborting"; return 1; }
#   NTFS_URL=`cat ${DATA_DIR}/${NTFS_REGION}.csv | grep NTFS | cut -d';' -f 2`
#   [[ $? != 0 ]] && { log_error "Could not find NTFS URL. Aborting"; return 1; }
#   wget --quiet --content-disposition --directory-prefix="${DATA_DIR}/ntfs" "${NTFS_URL}"
#   [[ $? != 0 ]] && { log_error "Could not download NTFS from ${NTFS_URL}. Aborting"; return 1; }
#   rm "${DATA_DIR}/${NTFS_REGION}.csv"
#   unzip -d "${DATA_DIR}/ntfs" "${DATA_DIR}/ntfs/*.zip"
#   [[ $? != 0 ]] && { log_error "Could not unzip NTFS from ${DATA_DIR}/ntfs. Aborting"; return 1; }
#   return 0
# }

########################### START ############################

while getopts "c:s:vVqh" opt; do
    case $opt in
        c) CONFIG_FILE="$OPTARG";;
        s) SETTINGS_FILE="$OPTARG";;
        v) version; exit 0 ;;
        V) VERBOSE=true ;;
        q) QUIET=true ;;
        h) usage; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
    esac
done

# Check that the variable $CONFIG_FILE is set and non-empty
[[ -z "${CONFIG_FILE+xxx}" ]] &&
{ echo -e "\e[91m config filename unset" >&2; echo "\e[0m" >&2; exit 1; }
[[ -z "$CONFIG_FILE" && "${CONFIG_FILE+xxx}" = "xxx" ]] &&
{ echo -e "\e[91m config filename set but empty" >&2; echo "\e[0m" >&2; exit 1; }

# Source $CONFIG_FILE
if [[ -f ${CONFIG_FILE} ]]; then
  log_info "Reading configuration '${CONFIG_FILE}'"
  source "${CONFIG_FILE}"
elif [[ -f "${SCRIPT_DIR}/${CONFIG_FILE}" ]]; then
  log_info "Reading '${SCRIPT_DIR}/${CONFIG_FILE}'"
  source "${SCRIPT_DIR}/${CONFIG_FILE}"
else
  log_error "Could not find ${CONFIG_FILE} in the current directory or in ${SCRIPT_DIR}"
  exit 1
fi

check_arguments
[[ $? != 0 ]] && { log_error "Invalid arguments. Aborting"; exit 1; }

check_requirements
[[ $? != 0 ]] && { log_error "Invalid requirements. Aborting"; exit 1; }

check_environment
[[ $? != 0 ]] && { log_error "Invalid environment. Aborting"; exit 1; }

start_docker
[[ $? != 0 ]] && { log_error "Could not start Elasticsearch. Aborting"; exit 1; }

# create_index
# [[ $? != 0 ]] && { log_error "Could not create Elasticsearch index. Aborting"; exit 1; }
# 
# create_mapping
# [[ $? != 0 ]] && { log_error "Could not create Elasticsearch mapping. Aborting"; exit 1; }
# 
# generate_input_stage_1
# [[ $? != 0 ]] && { log_error "Could not generate input file. Aborting"; exit 1; }
# 
# generate_input_stage_2
# [[ $? != 0 ]] && { log_error "Could not generate input file. Aborting"; exit 1; }
# 
# import_data
# [[ $? != 0 ]] && { log_error "Could not import data. Aborting"; exit 1; }


