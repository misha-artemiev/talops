package cloudflare

import (
	"context"
	"fmt"

	cf "github.com/cloudflare/cloudflare-go"
)

// DNSClient defines operations for shifting DNS traffic.
type DNSClient interface {
	GetARecordIP(ctx context.Context, zoneID, recordName string) (string, error)
	UpdateARecordIP(ctx context.Context, zoneID, recordName, newIP string) error
}

type client struct {
	api *cf.API
}

// NewClient initializes a new Cloudflare DNS client using the provided API token.
func NewClient(token string) (DNSClient, error) {
	api, err := cf.NewWithAPIToken(token)
	if err != nil {
		return nil, err
	}
	return &client{api: api}, nil
}

func (c *client) GetARecordIP(ctx context.Context, zoneID, recordName string) (string, error) {
	recs, _, err := c.api.ListDNSRecords(ctx, cf.ZoneIdentifier(zoneID), cf.ListDNSRecordsParams{
		Name: recordName,
		Type: "A",
	})
	if err != nil {
		return "", err
	}
	if len(recs) == 0 {
		return "", fmt.Errorf("no A record found for %s", recordName)
	}
	return recs[0].Content, nil
}

func (c *client) UpdateARecordIP(ctx context.Context, zoneID, recordName, newIP string) error {
	recs, _, err := c.api.ListDNSRecords(ctx, cf.ZoneIdentifier(zoneID), cf.ListDNSRecordsParams{
		Name: recordName,
		Type: "A",
	})
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return fmt.Errorf("no A record found for %s", recordName)
	}

	record := recs[0]
	if record.Content == newIP {
		return nil // already pointing to the target IP
	}

	_, err = c.api.UpdateDNSRecord(ctx, cf.ZoneIdentifier(zoneID), cf.UpdateDNSRecordParams{
		ID:      record.ID,
		Type:    "A",
		Name:    recordName,
		Content: newIP,
		Proxied: record.Proxied,
		TTL:     record.TTL,
	})
	return err
}
