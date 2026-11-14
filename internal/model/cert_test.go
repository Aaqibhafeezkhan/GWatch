package model

import (
	"testing"
	"time"
)

func TestCertExpiresWithin(t *testing.T) {
	now := time.Now()
	cert := func(lifetime time.Duration, daysLeft int) *CertInfo {
		notAfter := now.Add(time.Duration(daysLeft)*24*time.Hour + time.Hour)
		return &CertInfo{NotBefore: notAfter.Add(-lifetime), NotAfter: notAfter, DaysRemaining: daysLeft}
	}
	day := 24 * time.Hour
	cases := []struct {
		name     string
		c        *CertInfo
		warnDays int
		want     bool
	}{
		{"90-day certificate, 14 left", cert(90*day, 14), 14, true},
		{"90-day certificate, 15 left", cert(90*day, 15), 14, false},
		{"45-day certificate, 14 left", cert(45*day, 14), 14, true},
		{"six-day certificate just issued", cert(160*time.Hour, 6), 14, false},
		{"six-day certificate, 3 days left", cert(160*time.Hour, 3), 14, false},
		{"six-day certificate, 2 days left", cert(160*time.Hour, 2), 14, true},
		{"90-day certificate, a 60-day warning is kept", cert(90*day, 50), 60, true},
		{"45-day certificate, a 45-day warning is a third", cert(45*day, 20), 45, false},
		{"no lifetime known", &CertInfo{DaysRemaining: 10}, 14, true},
		{"expired", cert(160*time.Hour, -1), 14, true},
	}
	for _, tc := range cases {
		if got := tc.c.ExpiresWithin(tc.warnDays); got != tc.want {
			t.Errorf("%s: got %v, want %v (effective %d days)", tc.name, got, tc.want, tc.c.EffectiveWarnDays(tc.warnDays))
		}
	}
	var none *CertInfo
	if none.ExpiresWithin(14) {
		t.Error("no certificate is not expiring")
	}
}
