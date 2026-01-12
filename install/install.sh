service_name="incognita"

# Service names
prefix="projects_incognita_"
data_api_service="${prefix}data-api"
dashboard_service="${prefix}dashboard"
backup_scheduler_service="${prefix}data-backup-scheduler"

# Port numbers
data_api_port=5003
dashboard_port=5004

set -e  # Exit immediately if a command exits with a non-zero status

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

echo "Copying service file to systemd directory"
sudo cp install/${data_api_service}.service  /lib/systemd/system/${data_api_service}.service
sudo cp install/${dashboard_service}.service /lib/systemd/system/${dashboard_service}.service
sudo cp install/${backup_scheduler_service}.service /lib/systemd/system/${backup_scheduler_service}.service

echo "Setting permissions for the service file"
sudo chmod 644 /lib/systemd/system/${data_api_service}.service
sudo chmod 644 /lib/systemd/system/${dashboard_service}.service
sudo chmod 644 /lib/systemd/system/${backup_scheduler_service}.service

echo "Reloading systemd daemon"
sudo systemctl daemon-reload
sudo systemctl daemon-reexec

echo "Enabling services"
sudo systemctl enable ${data_api_service}.service
sudo systemctl enable ${dashboard_service}.service
sudo systemctl enable ${backup_scheduler_service}.service

echo "Restarting services"
sudo systemctl restart ${data_api_service}.service
sudo systemctl restart ${dashboard_service}.service
sudo systemctl restart ${backup_scheduler_service}.service

echo "Checking status of services"
sudo systemctl status ${data_api_service}.service --no-pager
sudo systemctl status ${dashboard_service}.service --no-pager
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
