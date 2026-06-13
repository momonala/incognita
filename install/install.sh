set -e

CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "✅ Installing uv (Python package manager)"
if ! command -v uv &> /dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
else
    echo "✅ uv is already installed. Updating to latest version."
    uv self update
fi

echo "✅ Installing project dependencies with uv"
uv sync

service_name=$(uv run config --project-name)
prefix="projects_${service_name}_"
data_api_service="${prefix}data-api"
app_service="projects_${service_name}"
backup_scheduler_service="${prefix}data-backup-scheduler"
data_api_port=$(uv run config --data-api-port)
dashboard_port=$(uv run config --dashboard-port)

echo "📋 Configuration:"
{
    uv run config --all | while IFS='=' read -r key value; do
        echo -e "   ${CYAN}${key}${NC}|${YELLOW}${value}${NC}"
    done
    echo -e "   ${CYAN}data_api_port${NC}|${YELLOW}${data_api_port}${NC}"
    echo -e "   ${CYAN}dashboard_port${NC}|${YELLOW}${dashboard_port}${NC}"
} | column -t -s '|'

echo "Copying service file to systemd directory"
sudo cp install/${data_api_service}.service  /lib/systemd/system/${data_api_service}.service
sudo cp install/${app_service}.service /lib/systemd/system/${app_service}.service
sudo cp install/${backup_scheduler_service}.service /lib/systemd/system/${backup_scheduler_service}.service

echo "Setting permissions for the service file"
sudo chmod 644 /lib/systemd/system/${data_api_service}.service
sudo chmod 644 /lib/systemd/system/${app_service}.service
sudo chmod 644 /lib/systemd/system/${backup_scheduler_service}.service

echo "Reloading systemd daemon"
sudo systemctl daemon-reload
sudo systemctl daemon-reexec

echo "Enabling services"
sudo systemctl enable ${data_api_service}.service
sudo systemctl enable ${app_service}.service
sudo systemctl enable ${backup_scheduler_service}.service

echo "Restarting services"
sudo systemctl restart ${data_api_service}.service
sudo systemctl restart ${app_service}.service
sudo systemctl restart ${backup_scheduler_service}.service

echo "Checking status of services"
sudo systemctl status ${data_api_service}.service --no-pager
sudo systemctl status ${app_service}.service --no-pager
sudo systemctl status ${backup_scheduler_service}.service --no-pager


echo "Adding Cloudflared service"
/home/mnalavadi/add_cloudflared_service.sh trace.mnalavadi.org ${data_api_port}
/home/mnalavadi/add_cloudflared_service.sh incognita.mnalavadi.org ${dashboard_port}

echo "Configuring Cloudflared DNS route"
cloudflared tunnel route dns raspberrypi-tunnel trace.mnalavadi.org
cloudflared tunnel route dns raspberrypi-tunnel incognita.mnalavadi.org

echo "Restarting Cloudflared service"
sudo systemctl restart cloudflared

echo "Setup completed successfully! 🎉"
