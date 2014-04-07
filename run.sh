#! /bin/bash
# Set up arguments.
KEY=""
SECRET=""
FILTER="filter-test.json"
MAPREDUCE="uitour"
VERBOSE=false
PULL="--local-only"
OUTPUT="my_mapreduce_results.out"

# Pull is a little more complicated, since we only want to do it if we have cached data.
if [ -d "../work/cache/saved_session" ]; then
  PULL="--local-only"
else
  PULL=""
fi

# Parse the arguments, kinda.
while [[ $1 ]]; do
  case $1 in
    -a|--auth)
      shift
      arr=(${1//:/ })
      KEY="-k ${arr[0]}"
      SECRET="-s ${arr[1]}"
      ;;
    -f|--for-real)
      FILTER="filter.json"
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo "  -f, --for-real        Use the less-specific non-test filter."
      echo "  -h, --help            Show this help."
      echo "  -m, --map-reduce ARG  Specify which mapreduce file to use."
      echo "  -o, --out ARG         Specify which file to output to."
      echo "  -p, --pull-data       Force the program to skip the cache, and pull the data again."
      echo "  -v, --verbose         Show more info about what's happening'."
      exit 0
      ;;
    -m|--map-reduce)
      shift
      MAPREDUCE=$1
      ;;
    -o|--out)
      shift
      OUTPUT=$1
      ;;
    -p|--pull-data)
      PULL=""
      ;;
    -v|--verbose)
      VERBOSE=true
      ;;
    *)
      echo '--> '"\`$1'" ;
      ;;
  esac
  shift
done

if $VERBOSE; then
  echo python -m mapreduce.job ../$MAPREDUCE.py --input-filter ../$FILTER --num-mappers 16 --num-reducers 4 --data-dir ../work/cache --work-dir ../work --output ../$OUTPUT $KEY $SECRET $PULL --bucket "telemetry-published-v1"
fi
python -m mapreduce.job ../$MAPREDUCE.py --input-filter ../$FILTER --num-mappers 16 --num-reducers 4 --data-dir ../work/cache --work-dir ../work --output ../$OUTPUT $KEY $SECRET $PULL --bucket "telemetry-published-v1"
