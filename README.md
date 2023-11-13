# Incognita

### To Do:
- Create Celery app for periodic tasks
    - duplicate raw_data/ to backup drive
    - refresh cache if data is updated

---
## Setup:

#### sync computer --> pi
- `rsync -avu raw_data/ mnalavadi@192.168.0.183:incognita/raw_data/`
- `rsync -avu cache/ mnalavadi@192.168.0.183:incognita/cache/`

#### sync pi --> computer
- `rsync -avu mnalavadi@192.168.0.183:incognita/raw_data/ raw_data/`
- `rsync -avu mnalavadi@192.168.0.183:incognita/cache/ cache/`

### Raspberry Pi setup:

#### Overland Server `systemd`
`/lib/systemd/system/projects_overland_server.service`
```
[Unit]
 Description=Incognita Overland Server
 After=multi-user.target

 [Service]
 WorkingDirectory=/home/mnalavadi/incognita
 Type=idle
 ExecStart=/usr/bin/python3 -m incognita.overland_server
 User=mnalavadi

 [Install]
 WantedBy=multi-user.target
```

#### Dashboard Service `systemd`
`/lib/systemd/system/projects_incognita_dashboard.service`
```
[Unit]
 Description=Incognita Dashboard Service
 After=multi-user.target

 [Service]
 WorkingDirectory=/home/mnalavadi/incognita
 Type=idle
 ExecStart=/usr/bin/python3 -m incognita.app
 User=mnalavadi

 [Install]
 WantedBy=multi-user.target
```

#### Start the services
```
sudo chmod 644 /lib/systemd/system/projects_overland_server.service
sudo chmod 644 /lib/systemd/system/projects_incognita_dashboard.service


sudo systemctl daemon-reload
sudo systemctl daemon-reexec

sudo systemctl enable projects_overland_server.service
sudo systemctl enable projects_incognita_dashboard.service

sudo reboot
```

#### View logs
```
journalctl -u projects_overland_server.service
journalctl -u projects_incognita_dashboard.service
```