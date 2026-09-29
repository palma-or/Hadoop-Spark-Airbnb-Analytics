/opt/spark/bin/spark-submit \
  --class it.unisa.hpc.spark.airbnbAnalysis.AirbnbAnalysis \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --supervise \
  --executor-memory 1G \
  ./AirbnbAnalysis.jar \
  ./input ./output_cluster
