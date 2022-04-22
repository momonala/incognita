# Incognita

### To Do:
- Create Celery app for periodic tasks
    - duplicate raw_data/ to backup drive
    - refresh cache if data is updated

---
## Setup:

#### sync computer --> pi
- `rsync  -avu  raw_data/  pi@192.168.0.184:incognita/raw_data/`
- `rsync  -avu  cache/     pi@192.168.0.184:incognita/cache/`

#### sync pi --> computer
- `rsync  -avu  rsync  -avu pi@192.168.0.184:incognita/raw_data/ raw_data/`
- `rsync  -avu  rsync  -avu pi@192.168.0.184:incognita/cache/    cache/`

### Raspberry Pi setup:

#### Overland Server `systemd`

`/lib/systemd/system/projects_overland_server.service`:
```
[Unit]
 Description=Incognita Overland Server
 After=multi-user.target

 [Service]
 WorkingDirectory=/home/pi/incognita
 Type=idle
 ExecStart=/usr/bin/python3 -m incognita.overland_server
 User=pi

 [Install]
 WantedBy=multi-user.target
```

#### Dashboard Service `systemd`
`/lib/systemd/system/projects_incognita_dashboard.service`:
```
[Unit]
 Description=Incognita Dashboard Service
 After=multi-user.target

 [Service]
 WorkingDirectory=/home/pi/incognita
 Type=idle
 ExecStart=/usr/bin/python3 -m incognita.app
 User=pi

 [Install]
 WantedBy=multi-user.target
```