.PHONY: start stop build restart logs ps test coverage lint clean \
        dbt-setup dbt-parse dbt-run dbt-test dbt-build \
        cloud-foundation-up cloud-seed-secrets cloud-foundation-down \
        cloud-plan cloud-up cloud-pause cloud-resume cloud-tunnels cloud-down

VENV_BIN = .venv/bin
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

# --- Cloud (GCP) — two Terraform layers: foundation (seed, run once) + app (routine) ---
TF_DIR         = infra/terraform
FOUNDATION_DIR = $(TF_DIR)/foundation
APP_DIR        = $(TF_DIR)/app
VM_NAME        = telemetry-stack
VM_ZONE        = europe-west3-a

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

cloud-up:          ## Deploy the app layer (network + VM); boots the stack
	cd $(APP_DIR) && terraform apply

cloud-pause:       ## Stop the VM (no compute charge; data + config preserved)
	gcloud compute instances stop $(VM_NAME) --zone=$(VM_ZONE)

cloud-resume:      ## Start the VM again; the stack auto-resumes
	gcloud compute instances start $(VM_NAME) --zone=$(VM_ZONE)

cloud-tunnels:     ## Open IAP tunnels to the dashboards (Grafana, Kafka-UI, ...)
	./$(TF_DIR)/iap-tunnels.sh $(VM_NAME) $(VM_ZONE)

cloud-down:        ## Destroy the app layer (foundation + secrets persist) → ~$0
	cd $(APP_DIR) && terraform destroy
