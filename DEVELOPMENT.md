# Deploy example app

Install `ko` and set `KO_DOCKER_REPO=gcr.io/[project-id]`

Set default region:

```
gcloud config set region us-east1
```

Deploy Cloud Run service:

```
gcloud run deploy delay --image=$(ko publish -P ./example)
```
