todo later...




run locally:

docker build -t kv-node:0.1.0 .

kind load docker-image kv-node:0.1.0 --name k8s

helm upgrade --install kv ./helm  --set image.repository=kv-node   --set image.tag=0.1.0   --set image.pullPolicy=IfNotPresent   --set replicaCount=3