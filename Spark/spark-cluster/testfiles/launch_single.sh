/opt/spark/bin/spark-submit \
  --class it.unisa.hpc.spark.airbnbAnalysis.AirbnbAnalysis \
  --master local \
  ./AirbnbAnalysis.jar \
  ./input ./output
