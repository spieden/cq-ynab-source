build:
	docker compose build

up:
	docker compose up -d

push: build
	docker push $REGISTRY/cq-ynab-source:latest
