package dbengine

import (
	log "github.com/sirupsen/logrus"
)

// DBSetting - sepcifies the various configurations of the database that are customizable
type DBSetting struct {
	DBDir                    string
	WalStrictModeOn          bool
	MemtableSizeByte         uint
	SStableDatablockSizeByte uint
	LogLevel                 log.Level
}

// DBConfig - configuration function for db setting
type DBConfig func(*DBSetting)

// ConfigDBDir - configures the DB directory location
func ConfigDBDir(dir string) DBConfig {
	return func(d *DBSetting) {
		d.DBDir = dir
	}
}

// ConfigWalStrictMode - configures if strict mode should be turned on or not for the wal (write-ahead-log).
// Strict mode means that every write to the wal file will be flushed to the storage device (instead of
// being buffered in the kernel's page cache, similar to calling `fsync` after every `write` syscall).
// It's advised that this setting only be turned on for mission critical application where no writes should
// be lost upon system failure.Enabling this comes with a signficant performance penalty for writing data.
func ConfigWalStrictMode(isOn bool) DBConfig {
	return func(d *DBSetting) {
		d.WalStrictModeOn = isOn
	}
}

// ConfigMemtableSizeByte - configures roughly how much data (in bytes) should be saved into the storage in memroy
// before it gets flushed into disk.
// Tuning this paramter could demonstrate different performance depending on the worklaod.
func ConfigMemtableSizeByte(size uint) DBConfig {
	return func(d *DBSetting) {
		d.MemtableSizeByte = size
	}
}

// ConfigSStableDatablockSizeByte - configures how much data (in bytes) should be stored for each data block in
// the underlying sstable file.
// Tuning this paramter could demonstrate different performance depending on the worklaod.
func ConfigSStableDatablockSizeByte(size uint) DBConfig {
	return func(d *DBSetting) {
		d.SStableDatablockSizeByte = size
	}
}

// ConfigLogLevel - configures the log level of the database, default to WARN
func ConfigLogLevel(level log.Level) DBConfig {
	return func(d *DBSetting) {
		d.LogLevel = level
	}
}

func defaultDBSetting() *DBSetting {
	return &DBSetting{
		DBDir:                    "./db",
		WalStrictModeOn:          false,
		MemtableSizeByte:         4 * 1024 * 1024, // 4 MB
		SStableDatablockSizeByte: 4 * 1024,        // 4 KB
		LogLevel:                 log.WarnLevel,
	}
}

// generateDBSetting - generates configuration for the database from the input configs
func generateDBSetting(configs ...DBConfig) *DBSetting {
	setting := defaultDBSetting()
	for _, config := range configs {
		config(setting)
	}
	return setting
}
