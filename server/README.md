```shell
mvn clean package -DskipTests

minikube image load artifact-server:latest

minikube mount /tmp:/mnt

kubectl apply -f pv.yaml
kubectl apply -f pvc.yaml

kubectl apply -f server-deploy.yaml

kubectl apply -f server-service.yaml

```

verify

```shell
kubectl get deploy

kubectl get pods 

kubectl logs -f deploy/artifacat-server

kubectl describe pod <pod-name>

minikube ssh

docker images
```

access from host laptop

```shell
minikube ip
```

`http://<minikube-ip>:30080`

Faster Development Workflow

```shell
mvn clean package -DskipTests

eval $(minikube docker-env)
docker images
docker rmi -f artifact-server:latest

eval $(minikube docker-env -u)
minikube image load <image-name>
kubectl rollout restart deployment <deployment-name>
```