# Set up arguments.
FILTER="filter-test.json"
MAPREDUCE="uitour"
VERBOSE=false
PULL="--local-only"

# Pull is a little more complicated, since we only want to do it if we have cached data.
if [ -d "../work/cache/saved_session" ]; then
  PULL="--local-only"
else
  PULL=""
fi

# Parse the arguments, kinda.
while [[ $1 ]]; do
  case $1 in
    --filter)
      shift
      FILTER=$1
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo "  -h, --help            Show this help."
      echo "  -m, --map-reduce ARG  Specify which mapreduce file to use."
      echo "  -p, --pull-data       Force the program to skip the cache, and pull the data again."
      echo "  -v, --verbose         Show more info about what's happening'."
      echo "  --filter              Use specified filter."
      exit 0
      ;;
    -m|--map-reduce)
      shift
      MAPREDUCE=$1
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
  echo python -m mapreduce.job ../$MAPREDUCE.py --input-filter ../$FILTER --num-mappers 16 --num-reducers 4 --data-dir ../work/cache --work-dir ../work --output ../my_mapreduce_results.out $PULL --bucket "telemetry-published-v1"
fi
python -m mapreduce.job ../$MAPREDUCE.py --input-filter ../$FILTER --num-mappers 16 --num-reducers 4 --data-dir ../work/cache --work-dir ../work --output ../my_mapreduce_results.out $PULL --bucket "telemetry-published-v1"
