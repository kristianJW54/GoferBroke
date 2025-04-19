package Network

import (
	"fmt"
	"net"
)

//========================================================
// Basic network checks for server
//========================================================

// Could add network type and reason
// struct {
//   Type
//   Reason
// }

type NodeNetworkReachability int

const (
	Unreachable NodeNetworkReachability = iota
	LoopbackOnly
	PrivateOnly
	PublicUnverified
	PublicReachable
	NATMapped
	RelayRequired
	PublicOpen
)

func ParseReachability(reach NodeNetworkReachability) (string, error) {

	switch reach {
	case Unreachable:
		return "Unreachable", nil
	case LoopbackOnly:
		return "LoopbackOnly", nil
	case PrivateOnly:
		return "PrivateOnly", nil
	case PublicUnverified:
		return "PublicUnverified", nil
	case PublicReachable:
		return "PublicReachable", nil
	case NATMapped:
		return "NATMapped", nil
	case RelayRequired:
		return "RelayRequired", nil
	case PublicOpen:
		return "PublicOpen", nil
	}

	return "", fmt.Errorf("un recognized node network reachability value: %d", reach)

}

func IsPrivate(ip net.IP) bool {
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified() || ip == nil
}

func DetermineInitialNetworkType(ip net.IP) NodeNetworkReachability {

	if IsPrivate(ip) {

		if ip.IsLoopback() {
			return LoopbackOnly
		}

		if ip.IsUnspecified() {
			return Unreachable
		}

		if ip.IsPrivate() {
			return PrivateOnly
		}

		if ip == nil {
			return Unreachable
		}

	}

	return PublicUnverified

}

func DetermineNodeNetworkType(configNetType int, ip net.IP) (NodeNetworkReachability, error) {

	initial := DetermineInitialNetworkType(ip)

	switch configNetType {
	case 0: //Undefined
		return initial, nil
	case 1: // Private
		switch initial {
		case PublicUnverified:
			return Unreachable, fmt.Errorf("network types mismatch - config specifies node network type as PRIVATE but detected PublicUnverified - check node address and network type")
		default:
			return initial, nil
		}
	case 2: // Public
		switch initial {
		case PublicUnverified:
			return initial, nil
		default:
			return Unreachable, fmt.Errorf("network types mismatch - config specifies node network type as PUBLIC but detected Private IP Address - check node address and network type")
		}
	case 3:
		switch initial {
		case LoopbackOnly:
			return initial, nil
		default:
			return Unreachable, fmt.Errorf("network type is configureed for loopback only")
		}
	}

	return Unreachable, fmt.Errorf("determine network type failed")

}

func DiscoverLocalIPFromUndefined(clusterNetworkTypePrivate bool, ip net.IP) (net.IP, error) {

	if !ip.IsUnspecified() {
		return ip, fmt.Errorf("IP address is not unspecified")
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("error getting network interfaces: %v", err)
	}

	for _, i := range ifaces {

		// Skip interfaces that are down or loopback
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {

			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			ip = ip.To4()
			if ip == nil {
				continue
			}

			if ip.IsLoopback() || ip.IsUnspecified() {
				continue
			}

			// Decide what to accept based on cluster type
			if clusterNetworkTypePrivate && ip.IsPrivate() {
				return ip, nil
			}

			if !clusterNetworkTypePrivate && !ip.IsPrivate() {
				return ip, nil
			}

		}
	}

	return nil, fmt.Errorf("no local IP address found")

}

func DetermineClusterNetworkType() {

}

// Upgrade checks here - STUN Check, Hole Punch check

// May want to get a list of ip addresses to lookup on the machine and try to resolve for
// If we have any public ones we can use OR try private ones for stun and traversal

//--------------------------
// Public IP

//========================================================
// NAT Traversal + STUN
//========================================================
