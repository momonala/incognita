service_name="incognita"
python_version="3.12"

set -e  # Exit immediately if a command exits with a non-zero status

echo "Creating conda environment: $service_name with Python $python_version"
if ! conda env list | grep -q "^$service_name\s"; then
    conda create -n $service_name python=$python_version -y
else
    echo "Conda environment '$service_name' already exists. Skipping creation."
fi

echo "Activating conda environment: $service_name"
source /home/mnalavadi/miniconda3/etc/profile.d/conda.sh
conda activate $service_name

echo "Installing required Python packages"
pip install -U poetry
poetry install --no-root

echo "Copying service file to systemd directory"
cp install/projects_overland_server.service  /lib/systemd/system/projects_overland_server.service
cp install/projects_incognita_dashboard.service /lib/systemd/system/projects_incognita_dashboard.service
cp install/projects_incognita_git_commit_scheduler.service /lib/systemd/system/projects_incognita_git_commit_scheduler.service

echo "Setting permissions for the service file"
sudo chmod 644 /lib/systemd/system/projects_overland_server.service
sudo chmod 644 /lib/systemd/system/projects_incognita_dashboard.service
sudo chmod 644 /lib/systemd/system/projects_incognita_git_commit_scheduler.service

echo "Reloading systemd daemon"
sudo systemctl daemon-reload
sudo systemctl daemon-reexec

echo "Enabling services"
sudo systemctl enable projects_overland_server.service
sudo systemctl enable projects_incognita_dashboard.service
sudo systemctl enable projects_incognita_git_commit_scheduler.service

echo "Restarting services"
sudo systemctl restart projects_overland_server.service
sudo systemctl restart projects_incognita_dashboard.service
sudo systemctl restart projects_incognita_git_commit_scheduler.service

echo "Checking status of services"
sudo systemctl status projects_overland_server.service --no-pager
sudo systemctl status projects_incognita_dashboard.service --no-pager

echo "Adding Cloudflared service"
/home/mnalavadi/add_cloudflared_service.sh trace.mnalavadi.org 5003
/home/mnalavadi/add_cloudflared_service.sh incognita.mnalavadi.org 5004

echo "Configuring Cloudflared DNS route"
cloudflared tunnel route dns raspberrypi-tunnel trace.mnalavadi.org
cloudflared tunnel route dns raspberrypi-tunnel incognita.mnalavadi.org

echo "Restarting Cloudflared service"
sudo systemctl restart cloudflared

echo "Setup completed successfully! 🎉"
