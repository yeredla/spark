spark-submit --master yarn --deploy-mode client --num-executors 4 --executor-cores 4 --class sayari /home/hadoop/sayari_sanctions_2.11-0.1.jar "/testing/ofac/ofac.jsonl" "/testing/uk/gbr.jsonl"
