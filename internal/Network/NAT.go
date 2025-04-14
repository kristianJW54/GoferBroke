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
	}

	return Unreachable, fmt.Errorf("determine network type failed")

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
