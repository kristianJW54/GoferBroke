package Network

import "net"

//========================================================
// Basic network checks for server
//========================================================

func IsPrivate(ip net.IP) bool {
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified()
}

// May want to get a list of ip addresses to lookup on the machine and try to resolve for
// If we have any public ones we can use OR try private ones for stun and traversal

//========================================================
// NAT Traversal + STUN
//========================================================
