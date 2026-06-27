.PHONY: start stop build restart logs ps test coverage lint clean \
        cloud-plan cloud-up cloud-secrets cloud-pause cloud-resume cloud-tunnels cloud-down

VENV_BIN = .venv/bin
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

# --- Cloud (GCP) — full stack on a Frankfurt VM, provisioned with Terraform ---
TF_DIR  = infra/terraform
VM_NAME = telemetry-stack
VM_ZONE = europe-west3-a

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

# === Cloud lifecycle (GCP, Terraform) =======================================
cloud-plan:        ## Preview the cloud infrastructure changes
	cd $(TF_DIR) && terraform plan

cloud-up:          ## Create the cloud infra + boot the stack VM (then run cloud-secrets)
	cd $(TF_DIR) && terraform apply

cloud-secrets:     ## Push secret values from .env into Secret Manager (run right after cloud-up)
	./$(TF_DIR)/push-secrets.sh .env

cloud-pause:       ## Stop the VM (no compute charge; data + config preserved)
	gcloud compute instances stop $(VM_NAME) --zone=$(VM_ZONE)

cloud-resume:      ## Start the VM again; the stack auto-resumes
	gcloud compute instances start $(VM_NAME) --zone=$(VM_ZONE)

cloud-tunnels:     ## Open IAP tunnels to the dashboards (Grafana, Kafka-UI, ...)
	./$(TF_DIR)/iap-tunnels.sh $(VM_NAME) $(VM_ZONE)

cloud-down:        ## Destroy ALL cloud infrastructure (back to $0)
	cd $(TF_DIR) && terraform destroy
