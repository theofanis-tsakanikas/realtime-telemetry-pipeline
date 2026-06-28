.PHONY: start stop build restart logs ps test coverage lint clean \
        dbt-setup dbt-parse dbt-run dbt-test dbt-build \
        k8s-images k8s-render k8s-apply k8s-delete \
        cloud-foundation-up cloud-seed-secrets cloud-foundation-down \
        cloud-plan cloud-up cloud-down

VENV_BIN = .venv/bin
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

# --- Cloud (GCP) — two Terraform layers: foundation (seed, run once) + app (routine) ---
TF_DIR         = infra/terraform
FOUNDATION_DIR = $(TF_DIR)/foundation
APP_DIR        = $(TF_DIR)/app

start:
	./run.sh up

stop:
	./run.sh down

build:
	./run.sh build

restart:
	./run.sh restart

logs:
	./run.sh logs

ps:
	./run.sh ps

test:
	$(VENV_BIN)/pytest tests/ -v

coverage:
	$(VENV_BIN)/pytest tests/ --cov --cov-report=term-missing --cov-report=html

lint:
	$(VENV_BIN)/ruff check scripts/ tests/ app/

clean:
	./run.sh down
	find data/checkpoints -mindepth 1 -type f -not -name '.gitkeep' -delete 2>/dev/null || true
	find data/logs -mindepth 1 -type f -not -name '.gitkeep' -delete 2>/dev/null || true

# === dbt — BigQuery analytics marts (own venv; pins clash with pyspark) ========
DBT_DIR  = dbt
DBT_VENV = .venv-dbt
DBT      = cd $(DBT_DIR) && DBT_PROFILES_DIR=. ../$(DBT_VENV)/bin/dbt

dbt-setup:  ## Create the dbt venv and install dbt-bigquery (run once)
	python3 -m venv $(DBT_VENV) && $(DBT_VENV)/bin/pip install -U pip -r $(DBT_DIR)/requirements.txt

dbt-parse:  ## Offline validation of the dbt project (no warehouse, no creds)
	$(DBT) parse

dbt-run:    ## Build the staging views + marts in BigQuery (needs ADC)
	$(DBT) run

dbt-test:   ## Run the dbt data tests against the built marts
	$(DBT) test

dbt-build:  ## run + test in dependency order (the usual refresh command)
	$(DBT) build

# === Kubernetes (GKE) — the stack as manifests =================================
K8S_DIR   = infra/k8s/base
AR_REPO   = europe-west3-docker.pkg.dev/realtime-telemetry-gcp/telemetry
# Generators read provisioning files outside the kustomize root, so relax the
# load restrictor. kubectl has kustomize built in.
KUSTOMIZE = kubectl kustomize --load-restrictor=LoadRestrictionsNone

k8s-images: ## Build simulator + spark images (amd64) and push to Artifact Registry
	gcloud auth configure-docker europe-west3-docker.pkg.dev --quiet
	docker build --platform linux/amd64 -f docker/Dockerfile.simulator -t $(AR_REPO)/simulator:latest .
	docker push $(AR_REPO)/simulator:latest
	docker build --platform linux/amd64 -f docker/Dockerfile.spark -t $(AR_REPO)/spark:latest .
	docker push $(AR_REPO)/spark:latest

k8s-render: ## Render the manifests to stdout (offline; what CI validates)
	$(KUSTOMIZE) $(K8S_DIR)

k8s-apply:  ## Deploy the stack to the current kubectl context (after get-credentials)
	$(KUSTOMIZE) $(K8S_DIR) | kubectl apply -f -

k8s-delete: ## Remove the stack from the cluster
	$(KUSTOMIZE) $(K8S_DIR) | kubectl delete -f -

# === Cloud — Layer 0 (foundation): run ONCE at setup, by the owner ===========
cloud-foundation-up:   ## Create WIF + deployer SA + runtime SA + secret containers (seed)
	cd $(FOUNDATION_DIR) && terraform init && terraform apply

cloud-seed-secrets:    ## Seed secret VALUES once from .env (persist across app deploys)
	./$(TF_DIR)/push-secrets.sh .env

cloud-foundation-down: ## Tear down the foundation too (only when fully decommissioning)
	cd $(FOUNDATION_DIR) && terraform destroy

# === Cloud — Layer 1 (app): routine spin-up / tear-down (CLI or CI) ===========
cloud-plan:        ## Preview the app-layer changes
	cd $(APP_DIR) && terraform plan

cloud-up:          ## Deploy the app layer (VPC + NAT + GKE Autopilot cluster)
	cd $(APP_DIR) && terraform apply

cloud-down:        ## Destroy the app layer (foundation + BigQuery + secrets persist) → ~$0
	cd $(APP_DIR) && terraform destroy
