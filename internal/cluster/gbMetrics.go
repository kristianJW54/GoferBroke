package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"log/slog"
	"math"
	"reflect"
	"time"
)

//===========================================================
// gopsutil system metrics

type systemMetrics struct {
	HostName       string
	Uptime         uint64
	Procs          uint64
	Os             string
	Platform       string
	PlatformFamily string
	HostID         string
	TotalMemory    uint64
	FreeMemory     uint64
	UsedMemory     uint64
	UsedPercent    float64
	CpuVendorID    string
	CpuFamily      string
	Cores          int32
	ModelName      string
	Mhz            float64
	CacheSize      int32
}

// New metrics will just be function calling the gopsutil functions and assigning
// We'll then need a background process ticker to update these and check if they go beyond a bound to then update
// e.g. if memory usage increases by 10% then we update or if used increased by 1000
// then we'll need to have a listener on that channel to handle the changes and update our own deltas

// While we do this - use the process to also check for other updates to our server like resolving addr and making sure it's up-to-date and reachable

func fieldNames[T any]() []string {
	var t T
	ty := reflect.TypeOf(t)
	if ty.Kind() == reflect.Pointer {
		ty = ty.Elem()
	}
	names := make([]string, 0, ty.NumField())
	for i := 0; i < ty.NumField(); i++ {
		sf := ty.Field(i)
		if sf.PkgPath == "" { // exported only
			names = append(names, sf.Name)
		}
	}
	return names
}

func newSystemMetrics() (*systemMetrics, []string) {

	h, _ := host.Info()
	c, _ := cpu.Info()
	v, _ := mem.VirtualMemory()

	sm := &systemMetrics{
		HostName:       h.Hostname,
		Uptime:         h.Uptime,
		Procs:          h.Procs,
		Os:             h.OS,
		Platform:       h.Platform,
		PlatformFamily: h.PlatformFamily,
		HostID:         h.HostID,
		TotalMemory:    v.Total,
		FreeMemory:     v.Free,
		UsedMemory:     v.Used,
		UsedPercent:    v.UsedPercent,
		CpuVendorID:    c[0].VendorID,
		CpuFamily:      c[0].Family,
		Cores:          c[0].Cores,
		ModelName:      c[0].ModelName,
		Mhz:            c[0].Mhz,
		CacheSize:      c[0].CacheSize,
	}

	fields := fieldNames[*systemMetrics]()

	return sm, fields

}

func getMemory() {

	v, _ := mem.VirtualMemory()

	fmt.Println(host.Info())

	fmt.Println(cpu.Info())

	// almost every return value is a struct
	fmt.Printf("Total: %v, Free:%v, Used:%v, UsedPercent:%f%%\n", v.Total, v.Free, v.Used, v.UsedPercent)

}

// percChanged returns true if new differs from old by >= threshold%.
// Example: threshold = 0.10 means "10%".
func percChanged(old, new uint64, threshold float64) bool {
	if old == 0 { // avoid div-by-zero, treat as always changed
		return new != 0
	}
	diff := float64(new) - float64(old)
	perc := diff / float64(old)
	if perc < 0 {
		perc = -perc
	}
	return perc >= threshold
}

// hasPercChanged returns true if new differs from old by >= threshold%.
// Example: threshold=0.10 means "10%".
func hasPercChanged(old, new float64, threshold float64) bool {
	if old == 0 {
		// if old is 0, treat any nonzero new as "changed"
		return new != 0
	}
	diff := (new - old) / old
	if diff < 0 {
		diff = -diff // absolute value
	}
	return diff >= threshold
}

// --------------------------------------------
// Server background tasks

// TODO Implement in future release
// Memory checker
// One for updating metric deltas
// Another for reaching memory limit and sending event

func (s *GBServer) runMetricCheck() {

	h, _ := host.Info()
	c, _ := cpu.Info()
	v, _ := mem.VirtualMemory()

	metrics := s.sm
	fields := s.smMap

	now := time.Now().Unix()

	for _, f := range fields {

		switch f {
		case "HostName":
			if h.Hostname != metrics.HostName {
				metrics.HostName = h.Hostname
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _HOST_,
					ValueType: D_STRING_TYPE,
					Version:   now,
					Value:     []byte(h.Hostname),
				})
				if err != nil {
					s.logger.Error("failed to update hostname during background run", slog.String("error", err.Error()))
				}
			}
		case "Uptime":
			if h.Uptime != metrics.Uptime {
				// not implemented
			}
		case "Procs":
			if h.Procs != metrics.Procs {
				// not implemented
			}
		case "Os":
			if h.OS != metrics.Os {
				// not implemented
			}
		case "Platform":
			if h.Platform != metrics.Platform {
				metrics.Platform = h.Platform
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _PLATFORM_,
					ValueType: D_STRING_TYPE,
					Version:   now,
					Value:     []byte(h.Platform),
				})
				if err != nil {
					s.logger.Error("failed to update platform during background run", slog.String("error", err.Error()))
				}
			}
		case "PlatformFamily":
			if h.PlatformFamily != metrics.PlatformFamily {
				metrics.PlatformFamily = h.PlatformFamily
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _PLATFORM_FAMILY_,
					ValueType: D_STRING_TYPE,
					Version:   now,
					Value:     []byte(h.PlatformFamily),
				})
				if err != nil {
					s.logger.Error("failed to update platform family during background run", slog.String("error", err.Error()))
				}
			}
		case "HostID":
			if h.HostID != metrics.HostID {
				metrics.HostID = h.HostID
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _HOST_ID_,
					ValueType: D_STRING_TYPE,
					Version:   now,
					Value:     []byte(h.HostID),
				})
				if err != nil {
					s.logger.Error("failed to update hostID during background run", slog.String("error", err.Error()))
				}
			}
		case "TotalMemory":
			if v.Total != metrics.TotalMemory {

				if percChanged(metrics.TotalMemory, v.Total, 0.10) {

					metrics.TotalMemory = v.Total

					total := make([]byte, 8)
					binary.BigEndian.PutUint64(total, v.Total)
					err := s.updateSelfInfo(&Delta{
						KeyGroup:  SYSTEM_DKG,
						Key:       _TOTAL_MEMORY_,
						ValueType: D_UINT64_TYPE,
						Version:   now,
						Value:     total,
					})
					if err != nil {
						s.logger.Error("failed to update total memory during background run", slog.String("error", err.Error()))
					}
				}

			}
		case "FreeMemory":
			if v.Free != metrics.FreeMemory {
				if percChanged(metrics.FreeMemory, v.Free, 0.10) {

					metrics.FreeMemory = v.Free

					free := make([]byte, 8)
					binary.BigEndian.PutUint64(free, v.Free)
					err := s.updateSelfInfo(&Delta{
						KeyGroup:  SYSTEM_DKG,
						Key:       _FREE_MEMORY_,
						ValueType: D_UINT64_TYPE,
						Version:   now,
						Value:     free,
					})
					if err != nil {
						s.logger.Error("failed to update free memory during background run", slog.String("error", err.Error()))
					}
				}
			}
		case "UsedMemory":
			if v.Used != metrics.UsedMemory {
				if percChanged(metrics.UsedMemory, v.Used, 0.10) {
					metrics.UsedMemory = v.Used

					used := make([]byte, 8)
					binary.BigEndian.PutUint64(used, v.Used)
					err := s.updateSelfInfo(&Delta{
						KeyGroup:  SYSTEM_DKG,
						Key:       _USED_MEMORY_,
						ValueType: D_UINT64_TYPE,
						Version:   now,
						Value:     used,
					})
					if err != nil {
						s.logger.Error("failed to update used memory during background run", slog.String("error", err.Error()))
					}
				}
			}
		case "UsedPercent":
			if v.UsedPercent != metrics.UsedPercent {
				if hasPercChanged(metrics.UsedPercent, v.UsedPercent, 0.10) {

					s.logger.Warn("percentage of memory used has increased/decreased by more than 10%",
						slog.Float64("previous", metrics.UsedPercent),
						slog.Float64("new", v.UsedPercent))

					metrics.UsedPercent = v.UsedPercent

					perc := make([]byte, 8)
					binary.BigEndian.PutUint64(perc, math.Float64bits(v.UsedPercent))
					err := s.updateSelfInfo(&Delta{
						KeyGroup:  SYSTEM_DKG,
						Key:       _MEM_PERC_,
						ValueType: D_FLOAT64_TYPE,
						Version:   now,
						Value:     perc,
					})
					if err != nil {
						s.logger.Error("failed to update used percentage during background run", slog.String("error", err.Error()))
					}
				}
			}
		case "CpuVendorID":
			if c[0].VendorID != metrics.CpuVendorID {
				// not implemented
			}
		case "CpuFamily":
			if c[0].Family != metrics.CpuFamily {
				// not implemented
			}
		case "Cores":
			if c[0].Cores != metrics.Cores {
				metrics.Cores = c[0].Cores
				cores := make([]byte, 4)
				binary.BigEndian.PutUint32(cores, uint32(c[0].Cores))
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _CPU_CORES_,
					ValueType: D_INT32_TYPE,
					Version:   now,
					Value:     cores,
				})
				if err != nil {
					s.logger.Error("failed to update cores during background run", slog.String("error", err.Error()))
				}
			}
		case "ModelName":
			if c[0].ModelName != metrics.ModelName {
				metrics.ModelName = c[0].ModelName
				err := s.updateSelfInfo(&Delta{
					KeyGroup:  SYSTEM_DKG,
					Key:       _CPU_MODE_NAME_,
					ValueType: D_STRING_TYPE,
					Version:   now,
					Value:     []byte(c[0].ModelName),
				})
				if err != nil {
					s.logger.Error("failed to update model name during background run", slog.String("error", err.Error()))
				}
			}
		case "Mhz":
			if c[0].Mhz != metrics.Mhz {
				// not implemented
			}
		case "CacheSize":
			if c[0].CacheSize != metrics.CacheSize {
				// not implemented
			}
		default:
			return // drop
		}

	}

}
