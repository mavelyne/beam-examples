#!/usr/bin/env bash
K8=$(kubectl cluster-info | grep master | sed 's/.*https/https/')
echo "Kubernetes Master = $K8"
CMD="/opt/spark/bin/spark-submit \
	--master k8s://$K8 \
	--deploy-mode cluster \
	--name beam-wordcount \
	--conf spark.executor.instances=1 \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
	--conf spark.kubernetes.container.image=mavelyne/beam-word-count \
	--conf spark.kubernetes.executor.volumes.hostPath.checkpoint.mount.path=/tmp \
	--conf spark.kubernetes.executor.volumes.hostPath.checkpoint.options.path=/tmp/word-count \
	--conf spark.kubernetes.executor.volumes.hostPath.checkpoint.options.type=Directory \
	--conf spark.kubernetes.executor.volumes.hostPath.checkpoint.mount.readOnly=false \
	--conf spark.kubernetes.driver.volumes.hostPath.checkpoint.mount.path=/tmp \
	--conf spark.kubernetes.driver.volumes.hostPath.checkpoint.options.path=/tmp/word-count \
	--conf spark.kubernetes.driver.volumes.hostPath.checkpoint.options.type=Directory \
	--conf spark.kubernetes.driver.volumes.hostPath.checkpoint.mount.readOnly=false \
	--class org.apache.beam.examples.KafkaWordCount  \
	/opt/spark/apps/word-count-beam-bundled-0.1.jar --runner=SparkRunner \
	--bootstrapServer=my-kafka:9092
"
echo $CMD
$CMD
