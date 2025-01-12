

cp install/projects_overland_server.service  /lib/systemd/system/projects_overland_server.service
cp install/projects_incognita_dashboard.service /lib/systemd/system/projects_incognita_dashboard.service

sudo chmod 644 /lib/systemd/system/projects_overland_server.service
sudo chmod 644 /lib/systemd/system/projects_incognita_dashboard.service


sudo systemctl daemon-reload
sudo systemctl daemon-reexec

sudo systemctl enable projects_overland_server.service
sudo systemctl enable projects_incognita_dashboard.service

