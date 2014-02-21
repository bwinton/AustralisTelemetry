python -m mapreduce.job ../uitour.py --input-filter ../filter-test.json --num-mappers 16 --num-reducers 4 --data-dir /mnt/telemetry/work --work-dir /mnt/telemetry/work --output /mnt/telemetry/my_mapreduce_results.out --bucket "telemetry-published-v1"

# if /cache exists --data-dir /mnt/telemetry/cache --local-only