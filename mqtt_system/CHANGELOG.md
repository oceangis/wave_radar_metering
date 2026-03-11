# Changelog

All notable changes to the Wave Monitoring System will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-01-12

### Added
- **Radar Auto-Retry Mechanism**: Automatic reconnection on device failure with configurable retry interval
  - Service remains running during radar disconnection (no restart loop)
  - Status reporting during wait period (`waiting_for_radars` state)
  - Configurable `radar_retry_interval` parameter (default: 30 seconds)
- **Comprehensive Deployment Documentation**: Complete `DEPLOYMENT.md` with step-by-step instructions
  - Hardware requirements and recommendations
  - Quick deployment guide
  - Database setup instructions
  - MQTT broker configuration
  - Troubleshooting guide
  - Performance optimization tips
  - Security recommendations
  - Backup and restore procedures
- **Configuration Template**: `system_config.yaml.example` with security best practices
  - Placeholder values for passwords and tokens
  - Detailed parameter documentation
  - Site-specific configuration guidance
- **Complete Systemd Service Files**: All 7 service files included in repository
  - `wave-collector.service` - Data acquisition
  - `wave-storage.service` - Data persistence
  - `wave-analyzer.service` - Spectral analysis
  - `wave-tide-analyzer.service` - Tidal analysis
  - `wave-web.service` - Web interface
  - `wave-thingsboard.service` - IoT platform bridge
  - `wave-ec800-thingsboard.service` - 4G modem bridge
- **Enhanced README.md**: Complete project overview with quick start guide
- **Project .gitignore**: Proper exclusion of logs, cache, and sensitive files

### Changed
- **Improved Error Handling**: Better error messages and recovery in collector service
- **Enhanced Logging**: More detailed debug information for troubleshooting
- **Configuration Structure**: Added `radar_retry_interval` to collection settings

### Fixed
- **Service Restart Loop**: Eliminated infinite restart cycle when radars are disconnected
  - Previous: Service exited immediately → systemd restarted every 10s
  - Now: Service stays alive and retries connection periodically
- **Resource Waste**: Reduced CPU/memory usage during radar unavailability

### Security
- **Password Protection**: Sensitive credentials removed from tracked config file
- **Configuration Template**: Safe example configuration for public repository

### Documentation
- Added comprehensive deployment guide (DEPLOYMENT.md)
- Updated project README with feature list and architecture
- Added inline documentation for retry mechanism
- Included troubleshooting section for common issues

---

## [2.0.0] - 2026-01-10

### Added
- **3-Radar Array Data Collection**: Parallel data acquisition from three VEGA radars
  - 6 Hz sampling rate with microsecond-level synchronization
  - Modbus/RS485 communication protocol
  - Real-time data publishing via MQTT
- **Wave Spectral Analysis**: Real-time frequency domain analysis
  - FFT-based energy spectrum calculation
  - Wave height parameters: Hm0, Hs, Hmax, H1/10, Hmean
  - Period parameters: Tp, Tm01, Tz, Te, Ts
  - Spectral moments: m-1, m0, m1, m2, m4
  - Frequency parameters: fp, fm, fz, fe
- **Directional Spectrum Analysis**: DIWASP algorithm integration
  - IMLM method for directional wave spectrum
  - Wave direction (θ) calculation
  - Directional spread analysis
  - Peak frequency direction
- **Zero-Crossing Analysis**: Time-domain statistical methods
  - Individual wave identification
  - Wave height and period statistics
  - Maximum wave (Hmax) tracking
  - 1/10 highest waves (H1/10)
- **Tidal Analysis**: UTide integration for harmonic analysis
  - Tidal constituent extraction
  - Tidal prediction capability
  - Separate analysis service
- **Web-Based Dashboard**: Real-time monitoring interface
  - Large display for key parameters (Hs, Tp, θ)
  - Live charts: energy spectrum, time series, trends
  - Polar plot for directional distribution
  - System status monitoring
  - WebSocket for real-time updates
- **Historical Data Management**: PostgreSQL-based data storage
  - Raw data retention: 30 days
  - Analysis data retention: 365 days
  - Automatic cleanup of old data
  - Time-series optimized queries
- **System Configuration Interface**: Web-based configuration editor
  - Real-time parameter adjustment
  - Configuration validation
  - Service restart management
- **MQTT Architecture**: Distributed messaging system
  - Topic-based data flow
  - QoS levels for reliability
  - Multiple subscriber support
- **Systemd Integration**: Production-ready service management
  - Automatic startup on boot
  - Service dependency management
  - Log management via journald
  - Resource limits configuration

### Technical Specifications
- **Sampling Rate**: 6 Hz (configurable)
- **Analysis Window**: 512 seconds (8.5 minutes)
- **Frequency Range**: 0.04 - 0.5 Hz (2-25 second periods)
- **Radar Synchronization**: < 1 ms time difference
- **Data Latency**: < 2 seconds (collection to display)
- **Database**: PostgreSQL 17 with JSONB support
- **Message Broker**: Mosquitto MQTT
- **Web Framework**: Flask with Flask-Sock (WebSocket)
- **Frontend**: Chart.js for visualization

### Dependencies
- Python 3.11+
- PostgreSQL 17
- Mosquitto MQTT Broker
- NumPy, SciPy (scientific computing)
- PySerial (serial communication)
- psycopg2 (PostgreSQL adapter)
- paho-mqtt (MQTT client)
- Flask, Flask-CORS, Flask-Sock (web framework)
- PyYAML (configuration)

---

## [1.0.0] - Initial Development

### Added
- Basic radar data collection prototype
- Simple data storage
- Initial analysis algorithms
- Command-line interface

---

## Release Notes

### V3.0.0 Highlights
This release focuses on **reliability and documentation** for production deployment:
- No more restart loops when radars are disconnected
- Complete deployment documentation for easy setup on new systems
- Security improvements with configuration templates
- Ready for multi-site deployment

### V2.0.0 Highlights
Complete rewrite with production-ready features:
- Full MQTT-based distributed architecture
- Real-time web dashboard with WebSocket
- Advanced wave analysis (spectral + directional)
- Database persistence with year-long retention
- Systemd integration for reliability

---

## Upgrade Guide

### From V2.0.0 to V3.0.0

**Configuration Changes:**
```yaml
# Add to config/system_config.yaml under 'collection' section:
collection:
  radar_retry_interval: 30  # seconds
```

**No Breaking Changes**: All existing functionality remains intact.

**Recommended Actions:**
1. Review `DEPLOYMENT.md` for best practices
2. Update `system_config.yaml` with retry interval setting
3. Review `.gitignore` if customizing
4. Update systemd service files from `systemd/` directory

**No Data Migration Required**: Database schema unchanged.

---

## Links
- **Repository**: https://github.com/oceangis/sensor_wave_radar
- **Issues**: https://github.com/oceangis/sensor_wave_radar/issues
- **Documentation**: See README.md and DEPLOYMENT.md

---

**Maintained by**: Wave Monitoring System Team
**License**: Proprietary
